using MappingFeed.Feed;
using MappingFeed.Osu;
using System.Data;
using System.Data.Common;
using System.Globalization;
using System.Text.Json.Nodes;
using Microsoft.EntityFrameworkCore;

namespace MappingFeed.Data;

public static class DatabaseSchemaUpdater
{
    public static async Task EnsureUpdatedAsync(
        MappingFeedDbContext db,
        OsuApiClient? osuApiClient = null,
        CancellationToken cancellationToken = default)
    {
        await db.Database.EnsureCreatedAsync(cancellationToken);

        if (!db.Database.IsSqlite())
            return;

        await EnsureColumnAsync(db, "subscribed_channels", "rulesets", "TEXT", cancellationToken);
        await EnsureColumnAsync(db, "subscribed_channels", "event_types", "TEXT", cancellationToken);
        await EnsureColumnAsync(db, "subscribed_channels", "group_id", "TEXT", cancellationToken);
        await EnsureColumnAsync(db, "beatmapset_events", "created_at", "TEXT", cancellationToken);
        await EnsureColumnAsync(db, "beatmapset_events", "discussion_id", "INTEGER", cancellationToken);
        await EnsureColumnAsync(db, "beatmapset_events", "mapper_user_id", "INTEGER", cancellationToken);
        await EnsureColumnAsync(db, "beatmapset_events", "rulesets", "TEXT", cancellationToken);
        await EnsureColumnAsync(db, "group_events", "raw_event", "TEXT", cancellationToken);
        await EnsureColumnAsync(db, "group_events", "user_name", "TEXT", cancellationToken);
        await EnsureColumnAsync(db, "group_events", "created_at", "TEXT", cancellationToken);
        await EnsureColumnAsync(db, "group_events", "group_name", "TEXT", cancellationToken);
        await EnsureColumnAsync(db, "group_events", "playmodes", "TEXT", cancellationToken);
        await BackfillNullTextColumnAsync(db, "group_events", "raw_event", "{}", cancellationToken);
        await BackfillNullTextColumnAsync(db, "beatmapset_events", "raw_event", "{}", cancellationToken);
        await BackfillBeatmapsetEventsFromRawEventAsync(db, cancellationToken);
        await BackfillBeatmapsetRulesetsFromOtherEventsAsync(db, cancellationToken);
        if (osuApiClient is not null)
            await BackfillBeatmapsetRulesetsFromApiAsync(db, osuApiClient, cancellationToken);
        await BackfillGroupEventsFromRawEventAsync(db, cancellationToken);

        await EnsureIndexAsync(db, "ix_beatmapset_events_created_at", "beatmapset_events", "created_at", cancellationToken);
        await EnsureIndexAsync(db, "ix_beatmapset_events_set_id", "beatmapset_events", "set_id", cancellationToken);
        await EnsureIndexAsync(db, "ix_group_events_created_at", "group_events", "created_at", cancellationToken);
        await EnsureIndexAsync(db, "ix_group_events_group_id", "group_events", "group_id", cancellationToken);
    }

    private static async Task EnsureColumnAsync(
        MappingFeedDbContext db,
        string tableName,
        string columnName,
        string columnType,
        CancellationToken cancellationToken)
    {
        await WithOpenConnectionAsync(db, cancellationToken, async (connection, token) =>
        {
            await using var pragma = connection.CreateCommand();
            pragma.CommandText = $"PRAGMA table_info({tableName});";

            await using var reader = await pragma.ExecuteReaderAsync(token);
            while (await reader.ReadAsync(token))
            {
                var existingColumn = reader["name"]?.ToString();
                if (string.Equals(existingColumn, columnName, StringComparison.OrdinalIgnoreCase))
                    return;
            }

            await using var alter = connection.CreateCommand();
            alter.CommandText = $"ALTER TABLE {tableName} ADD COLUMN {columnName} {columnType};";
            await alter.ExecuteNonQueryAsync(token);
        });
    }

    private static async Task BackfillNullTextColumnAsync(
        MappingFeedDbContext db,
        string tableName,
        string columnName,
        string replacement,
        CancellationToken cancellationToken)
    {
        await WithOpenConnectionAsync(db, cancellationToken, async (connection, token) =>
        {
            await using var command = connection.CreateCommand();
            command.CommandText = $"UPDATE {tableName} SET {columnName} = @replacement WHERE {columnName} IS NULL;";

            var parameter = command.CreateParameter();
            parameter.ParameterName = "@replacement";
            parameter.Value = replacement;
            command.Parameters.Add(parameter);

            await command.ExecuteNonQueryAsync(token);
        });
    }

    private static async Task EnsureIndexAsync(
        MappingFeedDbContext db,
        string indexName,
        string tableName,
        string columnName,
        CancellationToken cancellationToken)
    {
        await WithOpenConnectionAsync(db, cancellationToken, async (connection, token) =>
        {
            await using var command = connection.CreateCommand();
            command.CommandText = $"CREATE INDEX IF NOT EXISTS {indexName} ON {tableName} ({columnName});";
            await command.ExecuteNonQueryAsync(token);
        });
    }

    private static async Task BackfillBeatmapsetRulesetsFromOtherEventsAsync(
        MappingFeedDbContext db,
        CancellationToken cancellationToken)
    {
        await WithOpenConnectionAsync(db, cancellationToken, async (connection, token) =>
        {
            await using var command = connection.CreateCommand();
            command.CommandText = """
                UPDATE beatmapset_events AS target
                SET rulesets = (
                    SELECT source.rulesets
                    FROM beatmapset_events AS source
                    WHERE source.set_id = target.set_id
                      AND source.rulesets IS NOT NULL
                      AND TRIM(source.rulesets) <> ''
                    ORDER BY source.event_id DESC
                    LIMIT 1
                )
                WHERE (target.rulesets IS NULL OR TRIM(target.rulesets) = '')
                  AND EXISTS (
                    SELECT 1
                    FROM beatmapset_events AS source
                    WHERE source.set_id = target.set_id
                      AND source.rulesets IS NOT NULL
                      AND TRIM(source.rulesets) <> ''
                  );
                """;

            await command.ExecuteNonQueryAsync(token);
        });
    }

    private static async Task BackfillBeatmapsetRulesetsFromApiAsync(
        MappingFeedDbContext db,
        OsuApiClient osuApiClient,
        CancellationToken cancellationToken)
    {
        await WithOpenConnectionAsync(db, cancellationToken, async (connection, token) =>
        {
            var missingSetIds = new List<long>();

            await using (var select = connection.CreateCommand())
            {
                select.CommandText = """
                    SELECT DISTINCT set_id
                    FROM beatmapset_events
                    WHERE rulesets IS NULL OR TRIM(rulesets) = ''
                    ORDER BY set_id;
                    """;

                await using var reader = await select.ExecuteReaderAsync(token);
                while (await reader.ReadAsync(token))
                {
                    if (reader.IsDBNull(0))
                        continue;

                    missingSetIds.Add(reader.GetInt64(0));
                }
            }

            if (missingSetIds.Count == 0)
                return;

            await using var update = connection.CreateCommand();
            update.CommandText = """
                UPDATE beatmapset_events
                SET rulesets = @rulesets
                WHERE set_id = @set_id
                  AND (rulesets IS NULL OR TRIM(rulesets) = '');
                """;

            var rulesetsParameter = AddParameter(update, "@rulesets", DbType.String);
            var setIdParameter = AddParameter(update, "@set_id", DbType.Int64);

            foreach (var setId in missingSetIds)
            {
                IReadOnlyList<string> apiModes;
                try
                {
                    apiModes = await osuApiClient.GetBeatmapsetModesFailsafeAsync(
                        setId,
                        preferredUserId: null,
                        atOrBefore: null,
                        token);
                }
                catch
                {
                    continue;
                }

                var parsedRulesets = new HashSet<Ruleset>();
                foreach (var apiMode in apiModes)
                {
                    if (FeedEnumExtensions.TryParseRuleset(apiMode, out var parsedRuleset))
                        parsedRulesets.Add(parsedRuleset);
                }

                var serializedRulesets = FeedEnumExtensions.SerializeRulesets(parsedRulesets);
                if (string.IsNullOrWhiteSpace(serializedRulesets))
                    continue;

                rulesetsParameter.Value = serializedRulesets;
                setIdParameter.Value = setId;
                await update.ExecuteNonQueryAsync(token);
            }
        });
    }

    private static async Task BackfillBeatmapsetEventsFromRawEventAsync(
        MappingFeedDbContext db,
        CancellationToken cancellationToken)
    {
        await WithOpenConnectionAsync(db, cancellationToken, async (connection, token) =>
        {
            var updates = new List<BeatmapsetEventBackfillUpdate>();

            await using (var select = connection.CreateCommand())
            {
                select.CommandText = """
                    SELECT event_id, raw_event, created_at, discussion_id, mapper_user_id, rulesets
                    FROM beatmapset_events
                    WHERE raw_event IS NOT NULL
                      AND raw_event <> '{}'
                      AND (
                        created_at IS NULL OR
                        discussion_id IS NULL OR
                        mapper_user_id IS NULL OR
                        rulesets IS NULL OR
                        TRIM(rulesets) = ''
                      );
                    """;

                await using var reader = await select.ExecuteReaderAsync(token);
                while (await reader.ReadAsync(token))
                {
                    var eventId = reader.GetInt64(0);
                    var rawEvent = ReadNullableString(reader, 1);
                    if (string.IsNullOrWhiteSpace(rawEvent))
                        continue;

                    var existingCreatedAt = ReadNullableString(reader, 2);
                    var existingDiscussionId = ReadNullableInt64(reader, 3);
                    var existingMapperUserId = ReadNullableInt64(reader, 4);
                    var existingRulesets = ReadNullableString(reader, 5);

                    var root = TryParseRawObject(rawEvent);
                    if (root is null)
                        continue;

                    var createdAt = existingCreatedAt;
                    if (string.IsNullOrWhiteSpace(createdAt))
                    {
                        var parsedCreatedAt = TryParseCreatedAt(root);
                        if (parsedCreatedAt is not null)
                            createdAt = parsedCreatedAt.Value.ToString("O", CultureInfo.InvariantCulture);
                    }

                    var discussionId = existingDiscussionId;
                    if (discussionId is null)
                        discussionId = TryGetBeatmapsetDiscussionId(root);

                    var mapperUserId = existingMapperUserId;
                    if (mapperUserId is null)
                        mapperUserId = root.TryGetNestedInt64("beatmapset", "user_id");

                    var rulesets = existingRulesets;
                    if (string.IsNullOrWhiteSpace(rulesets))
                    {
                        var serializedRulesets = FeedEnumExtensions.SerializeRulesets(
                            FeedEnumExtensions.ExtractRulesets(rawEvent));

                        if (!string.IsNullOrWhiteSpace(serializedRulesets))
                            rulesets = serializedRulesets;
                    }

                    if (!string.Equals(existingCreatedAt, createdAt, StringComparison.Ordinal) ||
                        existingDiscussionId != discussionId ||
                        existingMapperUserId != mapperUserId ||
                        !string.Equals(existingRulesets, rulesets, StringComparison.Ordinal))
                    {
                        updates.Add(new BeatmapsetEventBackfillUpdate(
                            eventId,
                            createdAt,
                            discussionId,
                            mapperUserId,
                            rulesets));
                    }
                }
            }

            if (updates.Count == 0)
                return;

            await using var update = connection.CreateCommand();
            update.CommandText = """
                UPDATE beatmapset_events
                SET created_at = @created_at,
                    discussion_id = @discussion_id,
                    mapper_user_id = @mapper_user_id,
                    rulesets = @rulesets
                WHERE event_id = @event_id;
                """;

            var createdAtParameter = AddParameter(update, "@created_at", DbType.String);
            var discussionIdParameter = AddParameter(update, "@discussion_id", DbType.Int64);
            var mapperUserIdParameter = AddParameter(update, "@mapper_user_id", DbType.Int64);
            var rulesetsParameter = AddParameter(update, "@rulesets", DbType.String);
            var eventIdParameter = AddParameter(update, "@event_id", DbType.Int64);

            foreach (var row in updates)
            {
                createdAtParameter.Value = ToDbValue(row.CreatedAt);
                discussionIdParameter.Value = ToDbValue(row.DiscussionId);
                mapperUserIdParameter.Value = ToDbValue(row.MapperUserId);
                rulesetsParameter.Value = ToDbValue(row.Rulesets);
                eventIdParameter.Value = row.EventId;

                await update.ExecuteNonQueryAsync(token);
            }
        });
    }

    private static async Task BackfillGroupEventsFromRawEventAsync(
        MappingFeedDbContext db,
        CancellationToken cancellationToken)
    {
        await WithOpenConnectionAsync(db, cancellationToken, async (connection, token) =>
        {
            var updates = new List<GroupEventBackfillUpdate>();

            await using (var select = connection.CreateCommand())
            {
                select.CommandText = """
                    SELECT event_id, raw_event, user_name, created_at, group_name, playmodes
                    FROM group_events
                    WHERE raw_event IS NOT NULL
                      AND raw_event <> '{}'
                      AND (
                        user_name IS NULL OR TRIM(user_name) = '' OR
                        created_at IS NULL OR
                        group_name IS NULL OR TRIM(group_name) = '' OR
                        playmodes IS NULL OR TRIM(playmodes) = ''
                      );
                    """;

                await using var reader = await select.ExecuteReaderAsync(token);
                while (await reader.ReadAsync(token))
                {
                    var eventId = reader.GetInt64(0);
                    var rawEvent = ReadNullableString(reader, 1);
                    if (string.IsNullOrWhiteSpace(rawEvent))
                        continue;

                    var existingUserName = ReadNullableString(reader, 2);
                    var existingCreatedAt = ReadNullableString(reader, 3);
                    var existingGroupName = ReadNullableString(reader, 4);
                    var existingPlaymodes = ReadNullableString(reader, 5);

                    var root = TryParseRawObject(rawEvent);
                    if (root is null)
                        continue;

                    var userName = existingUserName;
                    if (string.IsNullOrWhiteSpace(userName))
                        userName = root.TryGetString("user_name") ?? root.TryGetNestedString("user", "username");

                    var createdAt = existingCreatedAt;
                    if (string.IsNullOrWhiteSpace(createdAt))
                    {
                        var parsedCreatedAt = TryParseCreatedAt(root);
                        if (parsedCreatedAt is not null)
                            createdAt = parsedCreatedAt.Value.ToString("O", CultureInfo.InvariantCulture);
                    }

                    var groupName = existingGroupName;
                    if (string.IsNullOrWhiteSpace(groupName))
                    {
                        groupName = root.TryGetString("group_name")
                            ?? root.TryGetNestedString("group", "short_name")
                            ?? root.TryGetNestedString("group", "name");
                    }

                    var playmodes = existingPlaymodes;
                    if (string.IsNullOrWhiteSpace(playmodes))
                        playmodes = TryExtractGroupPlaymodes(root);

                    if (!string.Equals(existingUserName, userName, StringComparison.Ordinal) ||
                        !string.Equals(existingCreatedAt, createdAt, StringComparison.Ordinal) ||
                        !string.Equals(existingGroupName, groupName, StringComparison.Ordinal) ||
                        !string.Equals(existingPlaymodes, playmodes, StringComparison.Ordinal))
                    {
                        updates.Add(new GroupEventBackfillUpdate(
                            eventId,
                            userName,
                            createdAt,
                            groupName,
                            playmodes));
                    }
                }
            }

            if (updates.Count == 0)
                return;

            await using var update = connection.CreateCommand();
            update.CommandText = """
                UPDATE group_events
                SET user_name = @user_name,
                    created_at = @created_at,
                    group_name = @group_name,
                    playmodes = @playmodes
                WHERE event_id = @event_id;
                """;

            var userNameParameter = AddParameter(update, "@user_name", DbType.String);
            var createdAtParameter = AddParameter(update, "@created_at", DbType.String);
            var groupNameParameter = AddParameter(update, "@group_name", DbType.String);
            var playmodesParameter = AddParameter(update, "@playmodes", DbType.String);
            var eventIdParameter = AddParameter(update, "@event_id", DbType.Int64);

            foreach (var row in updates)
            {
                userNameParameter.Value = ToDbValue(row.UserName);
                createdAtParameter.Value = ToDbValue(row.CreatedAt);
                groupNameParameter.Value = ToDbValue(row.GroupName);
                playmodesParameter.Value = ToDbValue(row.Playmodes);
                eventIdParameter.Value = row.EventId;

                await update.ExecuteNonQueryAsync(token);
            }
        });
    }

    private static JsonObject? TryParseRawObject(string rawEvent)
    {
        try
        {
            return JsonNode.Parse(rawEvent) as JsonObject;
        }
        catch
        {
            return null;
        }
    }

    private static DateTimeOffset? TryParseCreatedAt(JsonObject root)
    {
        var rawCreatedAt = root.TryGetString("created_at");
        if (string.IsNullOrWhiteSpace(rawCreatedAt))
            return null;

        return DateTimeOffset.TryParse(rawCreatedAt, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal, out var createdAt)
            ? createdAt
            : null;
    }

    private static long? TryGetBeatmapsetDiscussionId(JsonObject root)
    {
        return root.TryGetNestedInt64("discussion", "id")
            ?? root.TryGetNestedInt64("beatmap_discussion", "id")
            ?? root.TryGetNestedInt64("comment", "beatmap_discussion_id");
    }

    private static string? TryExtractGroupPlaymodes(JsonObject root)
    {
        if (root["playmodes"] is not JsonArray playmodesArray)
            return null;

        var modes = new List<string>();
        foreach (var modeNode in playmodesArray)
        {
            if (modeNode is null)
                continue;

            var rawMode = modeNode.ToString();
            if (FeedEnumExtensions.TryParseRuleset(rawMode, out var ruleset))
            {
                modes.Add(ruleset.ToCommandValue());
                continue;
            }

            if (!string.IsNullOrWhiteSpace(rawMode))
                modes.Add(rawMode.ToLowerInvariant());
        }

        return modes.Count == 0
            ? null
            : string.Join(", ", modes.Distinct());
    }

    private static string? ReadNullableString(DbDataReader reader, int ordinal)
    {
        return reader.IsDBNull(ordinal) ? null : reader.GetValue(ordinal)?.ToString();
    }

    private static long? ReadNullableInt64(DbDataReader reader, int ordinal)
    {
        if (reader.IsDBNull(ordinal))
            return null;

        var value = reader.GetValue(ordinal);
        if (value is long longValue)
            return longValue;

        if (value is int intValue)
            return intValue;

        return long.TryParse(value?.ToString(), NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed)
            ? parsed
            : null;
    }

    private static DbParameter AddParameter(DbCommand command, string name, DbType dbType)
    {
        var parameter = command.CreateParameter();
        parameter.ParameterName = name;
        parameter.DbType = dbType;
        command.Parameters.Add(parameter);
        return parameter;
    }

    private static object ToDbValue(object? value)
    {
        return value ?? DBNull.Value;
    }

    private static async Task WithOpenConnectionAsync(
        MappingFeedDbContext db,
        CancellationToken cancellationToken,
        Func<DbConnection, CancellationToken, Task> operation)
    {
        var connection = db.Database.GetDbConnection();
        var shouldClose = connection.State != ConnectionState.Open;

        if (shouldClose)
            await connection.OpenAsync(cancellationToken);

        try
        {
            await operation(connection, cancellationToken);
        }
        finally
        {
            if (shouldClose)
                await connection.CloseAsync();
        }
    }

    private sealed record BeatmapsetEventBackfillUpdate(
        long EventId,
        string? CreatedAt,
        long? DiscussionId,
        long? MapperUserId,
        string? Rulesets);

    private sealed record GroupEventBackfillUpdate(
        long EventId,
        string? UserName,
        string? CreatedAt,
        string? GroupName,
        string? Playmodes);
}
