using MappingFeed.Feed;
using MappingFeed.Osu;
using System.Data;
using System.Data.Common;
using System.Globalization;
using System.Text.Json;
using System.Text.Json.Nodes;
using Microsoft.EntityFrameworkCore;

namespace MappingFeed.Data;

public static class DatabaseSchemaUpdater
{
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);

    public static async Task EnsureUpdatedAsync(
        MappingFeedDbContext db,
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
        await EnsureColumnAsync(db, "beatmapset_events", "mapper_name", "TEXT", cancellationToken);
        await EnsureColumnAsync(db, "beatmapset_events", "beatmapset_title", "TEXT", cancellationToken);
        await EnsureColumnAsync(db, "beatmapset_events", "actor_username", "TEXT", cancellationToken);
        await EnsureColumnAsync(db, "beatmapset_events", "actor_avatar_url", "TEXT", cancellationToken);
        await EnsureColumnAsync(db, "beatmapset_events", "actor_badge", "TEXT", cancellationToken);
        await EnsureColumnAsync(db, "beatmapset_events", "actor_color", "TEXT", cancellationToken);
        await EnsureColumnAsync(db, "beatmapset_events", "rulesets", "TEXT", cancellationToken);
        await EnsureColumnAsync(db, "beatmapset_events", "ranked_history_json", "TEXT", cancellationToken);
        await EnsureColumnAsync(db, "group_events", "raw_event", "TEXT", cancellationToken);
        await EnsureColumnAsync(db, "group_events", "user_name", "TEXT", cancellationToken);
        await EnsureColumnAsync(db, "group_events", "actor_avatar_url", "TEXT", cancellationToken);
        await EnsureColumnAsync(db, "group_events", "actor_badge", "TEXT", cancellationToken);
        await EnsureColumnAsync(db, "group_events", "actor_color", "TEXT", cancellationToken);
        await EnsureColumnAsync(db, "group_events", "created_at", "TEXT", cancellationToken);
        await EnsureColumnAsync(db, "group_events", "group_name", "TEXT", cancellationToken);
        await EnsureColumnAsync(db, "group_events", "group_color", "TEXT", cancellationToken);
        await EnsureColumnAsync(db, "group_events", "playmodes", "TEXT", cancellationToken);
        await BackfillNullTextColumnAsync(db, "group_events", "raw_event", "{}", cancellationToken);
        await BackfillNullTextColumnAsync(db, "beatmapset_events", "raw_event", "{}", cancellationToken);
        await BackfillBeatmapsetEventsFromRawEventAsync(db, cancellationToken);
        await BackfillBeatmapsetRulesetsFromOtherEventsAsync(db, cancellationToken);
        await BackfillGroupEventsFromRawEventAsync(db, cancellationToken);

        await EnsureIndexAsync(db, "ix_beatmapset_events_created_at", "beatmapset_events", "created_at", cancellationToken);
        await EnsureIndexAsync(db, "ix_beatmapset_events_set_id", "beatmapset_events", "set_id", cancellationToken);
        await EnsureIndexAsync(db, "ix_group_events_created_at", "group_events", "created_at", cancellationToken);
        await EnsureIndexAsync(db, "ix_group_events_group_id", "group_events", "group_id", cancellationToken);
    }

    public static async Task RunApiBackfillAsync(
        MappingFeedDbContext db,
        OsuApiClient osuApiClient,
        TimeSpan apiThrottleDelay,
        int apiBatchSize,
        CancellationToken cancellationToken = default)
    {
        var context = new ApiBackfillContext(
            osuApiClient,
            apiThrottleDelay,
            Math.Clamp(apiBatchSize, 1, 512));

        await BackfillBeatmapsetRulesetsFromApiAsync(db, context, cancellationToken);
        await BackfillBeatmapsetMetadataFromApiAsync(db, context, cancellationToken);
        await BackfillBeatmapsetActorsFromApiAsync(db, context, cancellationToken);
        await BackfillBeatmapsetMessagesFromApiAsync(db, context, cancellationToken);
        await BackfillRankedHistorySnapshotsFromApiAsync(db, context, cancellationToken);
        await BackfillGroupProfilesFromApiAsync(db, context, cancellationToken);
        await BackfillGroupMetadataFromApiAsync(db, context, cancellationToken);
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
        ApiBackfillContext context,
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
                    apiModes = await context.InvokeAsync(
                        (api, ct) => api.GetBeatmapsetModesFailsafeAsync(
                            setId,
                            preferredUserId: null,
                            atOrBefore: null,
                            ct),
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
                    SELECT event_id, raw_event, created_at, discussion_id, mapper_user_id, mapper_name, beatmapset_title, actor_username, actor_color, rulesets
                    FROM beatmapset_events
                    WHERE raw_event IS NOT NULL
                      AND raw_event <> '{}'
                      AND (
                        created_at IS NULL OR
                        discussion_id IS NULL OR
                        mapper_user_id IS NULL OR
                        mapper_name IS NULL OR TRIM(mapper_name) = '' OR
                        beatmapset_title IS NULL OR TRIM(beatmapset_title) = '' OR
                        actor_username IS NULL OR TRIM(actor_username) = '' OR
                        actor_color IS NULL OR TRIM(actor_color) = '' OR
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
                    var existingMapperName = ReadNullableString(reader, 5);
                    var existingBeatmapsetTitle = ReadNullableString(reader, 6);
                    var existingActorUsername = ReadNullableString(reader, 7);
                    var existingActorColor = ReadNullableString(reader, 8);
                    var existingRulesets = ReadNullableString(reader, 9);

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

                    var mapperName = existingMapperName;
                    if (string.IsNullOrWhiteSpace(mapperName))
                        mapperName = root.TryGetNestedString("beatmapset", "creator");

                    var beatmapsetTitle = existingBeatmapsetTitle;
                    if (string.IsNullOrWhiteSpace(beatmapsetTitle))
                        beatmapsetTitle = BuildBeatmapsetTitle(root);

                    var actorUsername = existingActorUsername;
                    if (string.IsNullOrWhiteSpace(actorUsername))
                        actorUsername = root.TryGetNestedString("user", "username");

                    var actorColor = existingActorColor;
                    if (string.IsNullOrWhiteSpace(actorColor))
                        actorColor = TryExtractActorColor(root);

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
                        !string.Equals(existingMapperName, mapperName, StringComparison.Ordinal) ||
                        !string.Equals(existingBeatmapsetTitle, beatmapsetTitle, StringComparison.Ordinal) ||
                        !string.Equals(existingActorUsername, actorUsername, StringComparison.Ordinal) ||
                        !string.Equals(existingActorColor, actorColor, StringComparison.Ordinal) ||
                        !string.Equals(existingRulesets, rulesets, StringComparison.Ordinal))
                    {
                        updates.Add(new BeatmapsetEventBackfillUpdate(
                            eventId,
                            createdAt,
                            discussionId,
                            mapperUserId,
                            mapperName,
                            beatmapsetTitle,
                            actorUsername,
                            actorColor,
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
                    mapper_name = @mapper_name,
                    beatmapset_title = @beatmapset_title,
                    actor_username = @actor_username,
                    actor_color = @actor_color,
                    rulesets = @rulesets
                WHERE event_id = @event_id;
                """;

            var createdAtParameter = AddParameter(update, "@created_at", DbType.String);
            var discussionIdParameter = AddParameter(update, "@discussion_id", DbType.Int64);
            var mapperUserIdParameter = AddParameter(update, "@mapper_user_id", DbType.Int64);
            var mapperNameParameter = AddParameter(update, "@mapper_name", DbType.String);
            var beatmapsetTitleParameter = AddParameter(update, "@beatmapset_title", DbType.String);
            var actorUsernameParameter = AddParameter(update, "@actor_username", DbType.String);
            var actorColorParameter = AddParameter(update, "@actor_color", DbType.String);
            var rulesetsParameter = AddParameter(update, "@rulesets", DbType.String);
            var eventIdParameter = AddParameter(update, "@event_id", DbType.Int64);

            foreach (var row in updates)
            {
                createdAtParameter.Value = ToDbValue(row.CreatedAt);
                discussionIdParameter.Value = ToDbValue(row.DiscussionId);
                mapperUserIdParameter.Value = ToDbValue(row.MapperUserId);
                mapperNameParameter.Value = ToDbValue(row.MapperName);
                beatmapsetTitleParameter.Value = ToDbValue(row.BeatmapsetTitle);
                actorUsernameParameter.Value = ToDbValue(row.ActorUsername);
                actorColorParameter.Value = ToDbValue(row.ActorColor);
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
                    SELECT event_id, raw_event, user_name, actor_avatar_url, actor_badge, actor_color, created_at, group_name, group_color, playmodes
                    FROM group_events
                    WHERE raw_event IS NOT NULL
                      AND raw_event <> '{}'
                      AND (
                        user_name IS NULL OR TRIM(user_name) = '' OR
                        actor_avatar_url IS NULL OR TRIM(actor_avatar_url) = '' OR
                        actor_badge IS NULL OR TRIM(actor_badge) = '' OR
                        actor_color IS NULL OR TRIM(actor_color) = '' OR
                        created_at IS NULL OR
                        group_name IS NULL OR TRIM(group_name) = '' OR
                        group_color IS NULL OR TRIM(group_color) = '' OR
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
                    var existingActorAvatarUrl = ReadNullableString(reader, 3);
                    var existingActorBadge = ReadNullableString(reader, 4);
                    var existingActorColor = ReadNullableString(reader, 5);
                    var existingCreatedAt = ReadNullableString(reader, 6);
                    var existingGroupName = ReadNullableString(reader, 7);
                    var existingGroupColor = ReadNullableString(reader, 8);
                    var existingPlaymodes = ReadNullableString(reader, 9);

                    var root = TryParseRawObject(rawEvent);
                    if (root is null)
                        continue;

                    var userName = existingUserName;
                    if (string.IsNullOrWhiteSpace(userName))
                        userName = root.TryGetString("user_name") ?? root.TryGetNestedString("user", "username");

                    var actorAvatarUrl = existingActorAvatarUrl;
                    if (string.IsNullOrWhiteSpace(actorAvatarUrl))
                        actorAvatarUrl = root.TryGetNestedString("user", "avatar_url");

                    var actorBadge = existingActorBadge;
                    if (string.IsNullOrWhiteSpace(actorBadge))
                        actorBadge = root.TryGetNestedString("user", "title");

                    var actorColor = existingActorColor;
                    if (string.IsNullOrWhiteSpace(actorColor))
                        actorColor = TryExtractActorColor(root);

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

                    var groupColor = existingGroupColor;
                    if (string.IsNullOrWhiteSpace(groupColor))
                        groupColor = TryExtractGroupColor(root);

                    var playmodes = existingPlaymodes;
                    if (string.IsNullOrWhiteSpace(playmodes))
                        playmodes = TryExtractGroupPlaymodes(root);

                    if (!string.Equals(existingUserName, userName, StringComparison.Ordinal) ||
                        !string.Equals(existingActorAvatarUrl, actorAvatarUrl, StringComparison.Ordinal) ||
                        !string.Equals(existingActorBadge, actorBadge, StringComparison.Ordinal) ||
                        !string.Equals(existingActorColor, actorColor, StringComparison.Ordinal) ||
                        !string.Equals(existingCreatedAt, createdAt, StringComparison.Ordinal) ||
                        !string.Equals(existingGroupName, groupName, StringComparison.Ordinal) ||
                        !string.Equals(existingGroupColor, groupColor, StringComparison.Ordinal) ||
                        !string.Equals(existingPlaymodes, playmodes, StringComparison.Ordinal))
                    {
                        updates.Add(new GroupEventBackfillUpdate(
                            eventId,
                            userName,
                            actorAvatarUrl,
                            actorBadge,
                            actorColor,
                            createdAt,
                            groupName,
                            groupColor,
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
                    actor_avatar_url = @actor_avatar_url,
                    actor_badge = @actor_badge,
                    actor_color = @actor_color,
                    created_at = @created_at,
                    group_name = @group_name,
                    group_color = @group_color,
                    playmodes = @playmodes
                WHERE event_id = @event_id;
                """;

            var userNameParameter = AddParameter(update, "@user_name", DbType.String);
            var actorAvatarUrlParameter = AddParameter(update, "@actor_avatar_url", DbType.String);
            var actorBadgeParameter = AddParameter(update, "@actor_badge", DbType.String);
            var actorColorParameter = AddParameter(update, "@actor_color", DbType.String);
            var createdAtParameter = AddParameter(update, "@created_at", DbType.String);
            var groupNameParameter = AddParameter(update, "@group_name", DbType.String);
            var groupColorParameter = AddParameter(update, "@group_color", DbType.String);
            var playmodesParameter = AddParameter(update, "@playmodes", DbType.String);
            var eventIdParameter = AddParameter(update, "@event_id", DbType.Int64);

            foreach (var row in updates)
            {
                userNameParameter.Value = ToDbValue(row.UserName);
                actorAvatarUrlParameter.Value = ToDbValue(row.ActorAvatarUrl);
                actorBadgeParameter.Value = ToDbValue(row.ActorBadge);
                actorColorParameter.Value = ToDbValue(row.ActorColor);
                createdAtParameter.Value = ToDbValue(row.CreatedAt);
                groupNameParameter.Value = ToDbValue(row.GroupName);
                groupColorParameter.Value = ToDbValue(row.GroupColor);
                playmodesParameter.Value = ToDbValue(row.Playmodes);
                eventIdParameter.Value = row.EventId;

                await update.ExecuteNonQueryAsync(token);
            }
        });
    }

    private static async Task BackfillBeatmapsetMetadataFromApiAsync(
        MappingFeedDbContext db,
        ApiBackfillContext context,
        CancellationToken cancellationToken)
    {
        var setIds = await db.BeatmapsetEvents.AsNoTracking()
            .Where(x =>
                x.BeatmapsetTitle == null || x.BeatmapsetTitle == "" ||
                x.MapperName == null || x.MapperName == "")
            .Select(x => x.SetId)
            .Distinct()
            .OrderBy(x => x)
            .ToListAsync(cancellationToken);

        foreach (var setId in setIds)
        {
            OsuBeatmapsetInfo? beatmapset;
            try
            {
                beatmapset = await context.InvokeAsync(
                    (api, ct) => api.GetBeatmapsetAsync(setId, ct),
                    cancellationToken);
            }
            catch
            {
                continue;
            }

            var rows = await db.BeatmapsetEvents
                .Where(x =>
                    x.SetId == setId &&
                    (x.BeatmapsetTitle == null || x.BeatmapsetTitle == "" ||
                     x.MapperName == null || x.MapperName == ""))
                .ToListAsync(cancellationToken);

            var changed = false;
            foreach (var row in rows)
            {
                if (string.IsNullOrWhiteSpace(row.BeatmapsetTitle))
                {
                    var resolvedTitle = FirstNonEmpty(beatmapset?.Title) ?? $"Beatmapset {setId}";
                    if (!string.Equals(row.BeatmapsetTitle, resolvedTitle, StringComparison.Ordinal))
                    {
                        row.BeatmapsetTitle = resolvedTitle;
                        changed = true;
                    }
                }

                if (string.IsNullOrWhiteSpace(row.MapperName))
                {
                    var resolvedMapper = FirstNonEmpty(beatmapset?.Creator) ?? "Unknown";
                    if (!string.Equals(row.MapperName, resolvedMapper, StringComparison.Ordinal))
                    {
                        row.MapperName = resolvedMapper;
                        changed = true;
                    }
                }
            }

            if (changed)
                await db.SaveChangesAsync(cancellationToken);
        }
    }

    private static async Task BackfillBeatmapsetActorsFromApiAsync(
        MappingFeedDbContext db,
        ApiBackfillContext context,
        CancellationToken cancellationToken)
    {
        var userIds = await db.BeatmapsetEvents.AsNoTracking()
            .Where(x =>
                x.TriggeredBy != null &&
                (x.ActorUsername == null || x.ActorUsername == "" ||
                 x.ActorAvatarUrl == null || x.ActorAvatarUrl == "" ||
                 x.ActorBadge == null || x.ActorBadge == "" ||
                 x.ActorColor == null || x.ActorColor == "" ||
                 x.ActorBadge == "PROBATION"))
            .Select(x => x.TriggeredBy!.Value)
            .Distinct()
            .OrderBy(x => x)
            .ToListAsync(cancellationToken);

        foreach (var userId in userIds)
        {
            OsuUserProfileInfo? profile;
            try
            {
                profile = await context.InvokeAsync(
                    (api, ct) => api.GetUserProfileAsync(userId, ct),
                    cancellationToken);
            }
            catch
            {
                continue;
            }

            string? fallbackUserName = null;
            if (string.IsNullOrWhiteSpace(profile?.Username))
            {
                try
                {
                    fallbackUserName = await context.InvokeAsync(
                        (api, ct) => api.GetUserNameAsync(userId, ct),
                        cancellationToken);
                }
                catch
                {
                    fallbackUserName = null;
                }
            }

            var rows = await db.BeatmapsetEvents
                .Where(x =>
                    x.TriggeredBy == userId &&
                    (x.ActorUsername == null || x.ActorUsername == "" ||
                     x.ActorAvatarUrl == null || x.ActorAvatarUrl == "" ||
                     x.ActorBadge == null || x.ActorBadge == "" ||
                     x.ActorColor == null || x.ActorColor == "" ||
                     x.ActorBadge == "PROBATION"))
                .ToListAsync(cancellationToken);

            var changed = false;
            foreach (var row in rows)
            {
                var resolvedUserName = FirstNonEmpty(profile?.Username) ?? FirstNonEmpty(fallbackUserName);

                if (string.IsNullOrWhiteSpace(row.ActorUsername) && !string.IsNullOrWhiteSpace(resolvedUserName))
                {
                    row.ActorUsername = resolvedUserName;
                    changed = true;
                }

                if (string.IsNullOrWhiteSpace(row.ActorAvatarUrl) && !string.IsNullOrWhiteSpace(profile?.AvatarUrl))
                {
                    row.ActorAvatarUrl = profile.AvatarUrl;
                    changed = true;
                }

                if (string.IsNullOrWhiteSpace(row.ActorBadge) && !string.IsNullOrWhiteSpace(profile?.Badge))
                {
                    row.ActorBadge = profile.Badge;
                    changed = true;
                }

                if (string.IsNullOrWhiteSpace(row.ActorColor) && !string.IsNullOrWhiteSpace(profile?.Color))
                {
                    row.ActorColor = profile.Color;
                    changed = true;
                }
            }

            if (changed)
                await db.SaveChangesAsync(cancellationToken);
        }
    }

    private static async Task BackfillBeatmapsetMessagesFromApiAsync(
        MappingFeedDbContext db,
        ApiBackfillContext context,
        CancellationToken cancellationToken)
    {
        long cursor = 0;
        while (true)
        {
            var batch = await db.BeatmapsetEvents
                .Where(x =>
                    x.EventId > cursor &&
                    (x.Message == null || x.Message == "") &&
                    (x.EventType == FeedEventType.Nomination ||
                     x.EventType == FeedEventType.Qualification ||
                     x.EventType == FeedEventType.Disqualification ||
                     x.EventType == FeedEventType.NominationReset))
                .OrderBy(x => x.EventId)
                .Take(context.BatchSize)
                .ToListAsync(cancellationToken);

            if (batch.Count == 0)
                break;

            var changed = false;
            foreach (var row in batch)
            {
                var resolved = await ResolveBeatmapsetMessageForBackfillAsync(row, context, cancellationToken);
                var normalized = NormalizeMapMessage(row.EventType, resolved);

                if (!string.Equals(row.Message, normalized, StringComparison.Ordinal))
                {
                    row.Message = normalized;
                    changed = true;
                }
            }

            if (changed)
                await db.SaveChangesAsync(cancellationToken);

            cursor = batch[^1].EventId;
        }
    }

    private static async Task BackfillRankedHistorySnapshotsFromApiAsync(
        MappingFeedDbContext db,
        ApiBackfillContext context,
        CancellationToken cancellationToken)
    {
        var setIds = await db.BeatmapsetEvents.AsNoTracking()
            .Where(x =>
                x.EventType == FeedEventType.Ranked &&
                (x.RankedHistoryJson == null ||
                 x.RankedHistoryJson == "" ||
                 x.RankedHistoryJson.Contains("\"userId\":null") ||
                 x.RankedHistoryJson.Contains("\"username\":null") ||
                 x.RankedHistoryJson.Contains("\"userColor\":null") ||
                 !x.RankedHistoryJson.Contains("\"userColor\"")))
            .Select(x => x.SetId)
            .Distinct()
            .OrderBy(x => x)
            .ToListAsync(cancellationToken);

        var userNameCache = new Dictionary<long, string?>();
        var userProfileCache = new Dictionary<long, OsuUserProfileInfo?>();

        foreach (var setId in setIds)
        {
            IReadOnlyList<OsuBeatmapsetEventsEvent> completeHistory;
            try
            {
                completeHistory = await context.InvokeAsync(
                    (api, ct) => api.GetCompleteBeatmapsetEventHistoryAsync(setId, ct),
                    cancellationToken);
            }
            catch
            {
                continue;
            }

            var rankedRows = await db.BeatmapsetEvents
                .Where(x =>
                    x.SetId == setId &&
                    x.EventType == FeedEventType.Ranked &&
                    (x.RankedHistoryJson == null ||
                     x.RankedHistoryJson == "" ||
                     x.RankedHistoryJson.Contains("\"userId\":null") ||
                     x.RankedHistoryJson.Contains("\"username\":null") ||
                     x.RankedHistoryJson.Contains("\"userColor\":null") ||
                     !x.RankedHistoryJson.Contains("\"userColor\"")))
                .OrderBy(x => x.EventId)
                .ToListAsync(cancellationToken);

            var changed = false;
            foreach (var row in rankedRows)
            {
                var rankedHistory = await BuildRankedHistorySnapshotAsync(
                    row.EventId,
                    completeHistory,
                    context,
                    userProfileCache,
                    userNameCache,
                    cancellationToken);

                var serialized = rankedHistory.Count == 0
                    ? null
                    : JsonSerializer.Serialize(rankedHistory, JsonOptions);

                if (!string.Equals(row.RankedHistoryJson, serialized, StringComparison.Ordinal))
                {
                    row.RankedHistoryJson = serialized;
                    changed = true;
                }
            }

            if (changed)
                await db.SaveChangesAsync(cancellationToken);
        }
    }

    private static async Task BackfillGroupProfilesFromApiAsync(
        MappingFeedDbContext db,
        ApiBackfillContext context,
        CancellationToken cancellationToken)
    {
        var userIds = await db.GroupEvents.AsNoTracking()
            .Where(x =>
                x.UserName == null || x.UserName == "" ||
                x.ActorAvatarUrl == null || x.ActorAvatarUrl == "" ||
                x.ActorBadge == null || x.ActorBadge == "" ||
                x.ActorColor == null || x.ActorColor == "" ||
                x.ActorBadge == "PROBATION")
            .Select(x => x.UserId)
            .Distinct()
            .OrderBy(x => x)
            .ToListAsync(cancellationToken);

        foreach (var userId in userIds)
        {
            OsuUserProfileInfo? profile;
            try
            {
                profile = await context.InvokeAsync(
                    (api, ct) => api.GetUserProfileAsync(userId, ct),
                    cancellationToken);
            }
            catch
            {
                continue;
            }

            string? fallbackUserName = null;
            if (string.IsNullOrWhiteSpace(profile?.Username))
            {
                try
                {
                    fallbackUserName = await context.InvokeAsync(
                        (api, ct) => api.GetUserNameAsync(userId, ct),
                        cancellationToken);
                }
                catch
                {
                    fallbackUserName = null;
                }
            }

            var rows = await db.GroupEvents
                .Where(x =>
                    x.UserId == userId &&
                    (x.UserName == null || x.UserName == "" ||
                     x.ActorAvatarUrl == null || x.ActorAvatarUrl == "" ||
                     x.ActorBadge == null || x.ActorBadge == "" ||
                     x.ActorColor == null || x.ActorColor == "" ||
                     x.ActorBadge == "PROBATION"))
                .ToListAsync(cancellationToken);

            var changed = false;
            foreach (var row in rows)
            {
                var resolvedUserName = FirstNonEmpty(profile?.Username) ?? FirstNonEmpty(fallbackUserName);

                if (string.IsNullOrWhiteSpace(row.UserName) && !string.IsNullOrWhiteSpace(resolvedUserName))
                {
                    row.UserName = resolvedUserName;
                    changed = true;
                }

                if (string.IsNullOrWhiteSpace(row.ActorAvatarUrl) && !string.IsNullOrWhiteSpace(profile?.AvatarUrl))
                {
                    row.ActorAvatarUrl = profile.AvatarUrl;
                    changed = true;
                }

                if (string.IsNullOrWhiteSpace(row.ActorBadge) && !string.IsNullOrWhiteSpace(profile?.Badge))
                {
                    row.ActorBadge = profile.Badge;
                    changed = true;
                }

                if (string.IsNullOrWhiteSpace(row.ActorColor) && !string.IsNullOrWhiteSpace(profile?.Color))
                {
                    row.ActorColor = profile.Color;
                    changed = true;
                }
            }

            if (changed)
                await db.SaveChangesAsync(cancellationToken);
        }
    }

    private static async Task BackfillGroupMetadataFromApiAsync(
        MappingFeedDbContext db,
        ApiBackfillContext context,
        CancellationToken cancellationToken)
    {
        var groupIds = await db.GroupEvents.AsNoTracking()
            .Where(x =>
                x.GroupName == null || x.GroupName == "" ||
                x.GroupColor == null || x.GroupColor == "")
            .Select(x => x.GroupId)
            .Distinct()
            .OrderBy(x => x)
            .ToListAsync(cancellationToken);

        foreach (var groupId in groupIds)
        {
            OsuGroupInfo? groupInfo;
            try
            {
                groupInfo = await context.InvokeAsync(
                    (api, ct) => api.GetGroupInfoAsync(groupId, ct),
                    cancellationToken);
            }
            catch
            {
                continue;
            }

            var resolvedGroupName = FirstNonEmpty(groupInfo?.Name);
            var resolvedGroupColor = NormalizeColor(groupInfo?.Color);

            if (string.IsNullOrWhiteSpace(resolvedGroupName) && string.IsNullOrWhiteSpace(resolvedGroupColor))
                continue;

            var rows = await db.GroupEvents
                .Where(x =>
                    x.GroupId == groupId &&
                    (x.GroupName == null || x.GroupName == "" ||
                     x.GroupColor == null || x.GroupColor == ""))
                .ToListAsync(cancellationToken);

            if (rows.Count == 0)
                continue;

            var changed = false;
            foreach (var row in rows)
            {
                if (string.IsNullOrWhiteSpace(row.GroupName) && !string.IsNullOrWhiteSpace(resolvedGroupName))
                {
                    row.GroupName = resolvedGroupName;
                    changed = true;
                }

                if (string.IsNullOrWhiteSpace(row.GroupColor) && !string.IsNullOrWhiteSpace(resolvedGroupColor))
                {
                    row.GroupColor = resolvedGroupColor;
                    changed = true;
                }
            }

            if (changed)
                await db.SaveChangesAsync(cancellationToken);
        }
    }

    private static async Task<string?> ResolveBeatmapsetMessageForBackfillAsync(
        Data.Entities.BeatmapsetEvent beatmapsetEvent,
        ApiBackfillContext context,
        CancellationToken cancellationToken)
    {
        if (!string.IsNullOrWhiteSpace(beatmapsetEvent.Message))
            return beatmapsetEvent.Message;

        if (beatmapsetEvent.TriggeredBy is not null)
        {
            if (beatmapsetEvent.EventType is FeedEventType.Nomination or FeedEventType.Qualification)
            {
                var praiseOrHypeMessage = await context.InvokeAsync(
                    (api, ct) => api.GetLatestPraiseOrHypeMessageAsync(
                        beatmapsetEvent.SetId,
                        beatmapsetEvent.TriggeredBy.Value,
                        beatmapsetEvent.CreatedAt,
                        ct),
                    cancellationToken);
                if (!string.IsNullOrWhiteSpace(praiseOrHypeMessage))
                    return praiseOrHypeMessage;
            }

            if (beatmapsetEvent.EventType is FeedEventType.Disqualification or FeedEventType.NominationReset)
            {
                var discussionMessageByUser = await context.InvokeAsync(
                    (api, ct) => api.GetLatestDiscussionMessageByUserAsync(
                        beatmapsetEvent.SetId,
                        beatmapsetEvent.TriggeredBy.Value,
                        beatmapsetEvent.CreatedAt,
                        ct),
                    cancellationToken);
                if (!string.IsNullOrWhiteSpace(discussionMessageByUser))
                    return discussionMessageByUser;
            }
        }

        if (beatmapsetEvent.PostId is not null || beatmapsetEvent.DiscussionId is not null)
        {
            var discussionMessage = await context.InvokeAsync(
                (api, ct) => api.GetBeatmapsetDiscussionMessageAsync(
                    beatmapsetEvent.SetId,
                    beatmapsetEvent.PostId,
                    beatmapsetEvent.DiscussionId,
                    ct),
                cancellationToken);

            if (!string.IsNullOrWhiteSpace(discussionMessage))
                return discussionMessage;
        }

        return null;
    }

    private static string? NormalizeMapMessage(FeedEventType eventType, string? message)
    {
        if (eventType is not (FeedEventType.Nomination or FeedEventType.Qualification or FeedEventType.Disqualification or FeedEventType.NominationReset))
            return null;

        if (string.IsNullOrWhiteSpace(message))
            return null;

        var normalized = string.Join(' ', message
            .Split(['\r', '\n'], StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries));

        if (string.IsNullOrWhiteSpace(normalized))
            return null;

        return normalized.Length <= 220
            ? normalized
            : normalized[..Math.Max(0, 217)] + "...";
    }

    private static async Task<IReadOnlyList<RankedHistorySnapshot>> BuildRankedHistorySnapshotAsync(
        long eventId,
        IReadOnlyList<OsuBeatmapsetEventsEvent> completeHistory,
        ApiBackfillContext context,
        Dictionary<long, OsuUserProfileInfo?> userProfileCache,
        Dictionary<long, string?> userNameCache,
        CancellationToken cancellationToken)
    {
        var relevantHistory = completeHistory
            .Where(x => x.Id <= eventId)
            .Select(x => new
            {
                Event = x,
                Type = MapHistoryEventType(x.Type),
            })
            .Where(x => x.Type is not null)
            .Select(x => new RankedHistoryEvent(x.Event, x.Type!.Value))
            .OrderBy(x => x.Event.Id)
            .ToList();

        if (relevantHistory.Count == 0)
            return [];

        var historyWithActors = relevantHistory
            .Select((x, index) => new RankedHistoryEntry(
                x.Event,
                x.Type,
                ResolveHistoryUserId(relevantHistory, index)))
            .ToList();
        historyWithActors = CoalesceRankedHistory(historyWithActors);

        var lastQualification = historyWithActors.LastOrDefault(x => x.Type == FeedEventType.Qualification);
        var trimmedHistory = historyWithActors
            .OrderByDescending(x => x.Event.Id)
            .Take(8)
            .OrderBy(x => x.Event.Id)
            .ToList();

        if (lastQualification is not null &&
            trimmedHistory.All(x => x.Event.Id != lastQualification.Event.Id))
        {
            trimmedHistory.RemoveAt(0);
            trimmedHistory.Add(lastQualification);
            trimmedHistory = trimmedHistory
                .OrderBy(x => x.Event.Id)
                .ToList();
        }

        var actions = new List<RankedHistorySnapshot>();
        foreach (var historyEvent in trimmedHistory)
        {
            string? userName = null;
            string? userColor = null;
            if (historyEvent.UserId is not null)
            {
                var userId = historyEvent.UserId.Value;

                if (!userProfileCache.TryGetValue(userId, out var profile))
                {
                    profile = await context.InvokeAsync(
                        (api, ct) => api.GetUserProfileAsync(userId, ct),
                        cancellationToken);
                    userProfileCache[userId] = profile;
                }

                userColor = profile?.Color;

                if (!userNameCache.TryGetValue(userId, out userName))
                {
                    userName = FirstNonEmpty(profile?.Username);
                    if (string.IsNullOrWhiteSpace(userName))
                    {
                        userName = await context.InvokeAsync(
                            (api, ct) => api.GetUserNameAsync(userId, ct),
                            cancellationToken);
                    }

                    userNameCache[userId] = userName;
                }
            }

            actions.Add(new RankedHistorySnapshot(
                historyEvent.Type,
                historyEvent.UserId,
                string.IsNullOrWhiteSpace(userName) ? null : userName,
                FirstNonEmpty(userColor)));
        }

        return actions;
    }

    private static List<RankedHistoryEntry> CoalesceRankedHistory(IReadOnlyList<RankedHistoryEntry> entries)
    {
        var ordered = entries
            .OrderBy(x => x.Event.Id)
            .ToList();

        var coalesced = new List<RankedHistoryEntry>();
        foreach (var entry in ordered)
        {
            if (coalesced.Count == 0)
            {
                coalesced.Add(entry);
                continue;
            }

            var previous = coalesced[^1];

            if (entry.Type == FeedEventType.Qualification &&
                previous.Type == FeedEventType.Nomination &&
                IsLikelyLinkedNominationAndQualification(previous, entry))
            {
                coalesced[^1] = entry;
                continue;
            }

            if (AreLikelyDuplicateHistoryEntries(previous, entry))
                continue;

            coalesced.Add(entry);
        }

        return coalesced;
    }

    private static bool IsLikelyLinkedNominationAndQualification(
        RankedHistoryEntry nomination,
        RankedHistoryEntry qualification)
    {
        if (nomination.UserId is not null &&
            qualification.UserId is not null &&
            nomination.UserId != qualification.UserId)
        {
            return false;
        }

        return IsCloseInTime(nomination.Event.CreatedAt, qualification.Event.CreatedAt, TimeSpan.FromMinutes(2));
    }

    private static bool AreLikelyDuplicateHistoryEntries(
        RankedHistoryEntry previous,
        RankedHistoryEntry current)
    {
        if (previous.Type != current.Type)
            return false;

        if (current.Type == FeedEventType.Nomination)
            return false;

        if (previous.UserId is not null &&
            current.UserId is not null &&
            previous.UserId != current.UserId)
        {
            return false;
        }

        return IsCloseInTime(previous.Event.CreatedAt, current.Event.CreatedAt, TimeSpan.FromMinutes(2));
    }

    private static bool IsCloseInTime(
        DateTimeOffset? earlier,
        DateTimeOffset? later,
        TimeSpan maxGap)
    {
        if (earlier is null || later is null)
            return false;

        var delta = later.Value - earlier.Value;
        if (delta < TimeSpan.Zero)
            return false;

        return delta <= maxGap;
    }

    private static FeedEventType? MapHistoryEventType(string rawType)
    {
        return rawType.Trim().ToLowerInvariant() switch
        {
            "nominate" => FeedEventType.Nomination,
            "nomination_reset" => FeedEventType.NominationReset,
            "qualify" => FeedEventType.Qualification,
            "disqualify" => FeedEventType.Disqualification,
            _ => null,
        };
    }

    private static long? ResolveHistoryUserId(
        IReadOnlyList<RankedHistoryEvent> relevantHistory,
        int index)
    {
        var historyEvent = relevantHistory[index];
        var sourceEvent = historyEvent.Event;

        if (sourceEvent.UserId is not null)
            return sourceEvent.UserId.Value;

        if (historyEvent.Type != FeedEventType.Qualification)
            return null;

        for (var i = index - 1; i >= 0; i--)
        {
            var (candidateEvent, candidateType) = relevantHistory[i];
            if (candidateType != FeedEventType.Nomination || candidateEvent.UserId is null)
                continue;

            if (sourceEvent.CreatedAt is not null && candidateEvent.CreatedAt is not null)
            {
                var delta = sourceEvent.CreatedAt.Value - candidateEvent.CreatedAt.Value;
                if (delta < TimeSpan.Zero || delta > TimeSpan.FromMinutes(2))
                    continue;
            }

            return candidateEvent.UserId.Value;
        }

        return null;
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

    private static string? BuildBeatmapsetTitle(JsonObject root)
    {
        var artist = root.TryGetNestedString("beatmapset", "artist");
        var title = root.TryGetNestedString("beatmapset", "title");

        if (!string.IsNullOrWhiteSpace(artist) && !string.IsNullOrWhiteSpace(title))
            return $"{artist} - {title}";

        return FirstNonEmpty(title);
    }

    private static string? FirstNonEmpty(string? value)
    {
        return string.IsNullOrWhiteSpace(value) ? null : value;
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

    private static string? TryExtractGroupColor(JsonObject root)
    {
        var rawColor = root.TryGetString("group_color", "group_colour")
            ?? root.TryGetNestedString("group", "color")
            ?? root.TryGetNestedString("group", "colour");

        return NormalizeColor(rawColor);
    }

    private static string? TryExtractActorColor(JsonObject root)
    {
        var rawColor = root.TryGetString("user_color", "user_colour")
            ?? root.TryGetNestedString("user", "color")
            ?? root.TryGetNestedString("user", "colour");

        var normalized = NormalizeColor(rawColor);
        if (!string.IsNullOrWhiteSpace(normalized))
            return normalized;

        if (root["user"]?["groups"] is not JsonArray groups)
            return null;

        foreach (var group in groups.OfType<JsonObject>())
        {
            var groupColor = NormalizeColor(group.TryGetString("color", "colour"));
            if (!string.IsNullOrWhiteSpace(groupColor))
                return groupColor;
        }

        return null;
    }

    private static string? NormalizeColor(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
            return null;

        var trimmed = value.Trim();
        if (trimmed.StartsWith('#'))
            trimmed = trimmed[1..];

        if (trimmed.Length == 3 && trimmed.All(Uri.IsHexDigit))
            trimmed = string.Concat(trimmed.Select(x => $"{x}{x}"));

        if (trimmed.Length != 6 || !trimmed.All(Uri.IsHexDigit))
            return null;

        return $"#{trimmed.ToUpperInvariant()}";
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

    private sealed class ApiBackfillContext(
        OsuApiClient apiClient,
        TimeSpan throttleDelay,
        int batchSize)
    {
        public int BatchSize { get; } = batchSize;

        private DateTimeOffset _nextAllowedRequestAtUtc = DateTimeOffset.MinValue;

        public async Task<T> InvokeAsync<T>(
            Func<OsuApiClient, CancellationToken, Task<T>> operation,
            CancellationToken cancellationToken)
        {
            if (throttleDelay > TimeSpan.Zero)
            {
                var now = DateTimeOffset.UtcNow;
                if (_nextAllowedRequestAtUtc > now)
                    await Task.Delay(_nextAllowedRequestAtUtc - now, cancellationToken);
            }

            var result = await operation(apiClient, cancellationToken);
            _nextAllowedRequestAtUtc = DateTimeOffset.UtcNow + throttleDelay;
            return result;
        }
    }

    private sealed record BeatmapsetEventBackfillUpdate(
        long EventId,
        string? CreatedAt,
        long? DiscussionId,
        long? MapperUserId,
        string? MapperName,
        string? BeatmapsetTitle,
        string? ActorUsername,
        string? ActorColor,
        string? Rulesets);

    private sealed record GroupEventBackfillUpdate(
        long EventId,
        string? UserName,
        string? ActorAvatarUrl,
        string? ActorBadge,
        string? ActorColor,
        string? CreatedAt,
        string? GroupName,
        string? GroupColor,
        string? Playmodes);

    private sealed record RankedHistorySnapshot(
        FeedEventType Action,
        long? UserId,
        string? Username,
        string? UserColor);

    private sealed record RankedHistoryEvent(OsuBeatmapsetEventsEvent Event, FeedEventType Type);

    private sealed record RankedHistoryEntry(OsuBeatmapsetEventsEvent Event, FeedEventType Type, long? UserId);
}
