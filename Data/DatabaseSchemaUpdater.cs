using System.Data;
using Microsoft.EntityFrameworkCore;

namespace MappingFeed.Data;

public static class DatabaseSchemaUpdater
{
    public static async Task EnsureUpdatedAsync(MappingFeedDbContext db, CancellationToken cancellationToken = default)
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
        var connection = db.Database.GetDbConnection();
        var shouldClose = connection.State != ConnectionState.Open;

        if (shouldClose)
            await connection.OpenAsync(cancellationToken);

        try
        {
            await using var pragma = connection.CreateCommand();
            pragma.CommandText = $"PRAGMA table_info({tableName});";

            await using var reader = await pragma.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                var existingColumn = reader["name"]?.ToString();
                if (string.Equals(existingColumn, columnName, StringComparison.OrdinalIgnoreCase))
                    return;
            }

            await using var alter = connection.CreateCommand();
            alter.CommandText = $"ALTER TABLE {tableName} ADD COLUMN {columnName} {columnType};";
            await alter.ExecuteNonQueryAsync(cancellationToken);
        }
        finally
        {
            if (shouldClose)
                await connection.CloseAsync();
        }
    }

    private static async Task BackfillNullTextColumnAsync(
        MappingFeedDbContext db,
        string tableName,
        string columnName,
        string replacement,
        CancellationToken cancellationToken)
    {
        var connection = db.Database.GetDbConnection();
        var shouldClose = connection.State != ConnectionState.Open;

        if (shouldClose)
            await connection.OpenAsync(cancellationToken);

        try
        {
            await using var command = connection.CreateCommand();
            command.CommandText = $"UPDATE {tableName} SET {columnName} = @replacement WHERE {columnName} IS NULL;";

            var parameter = command.CreateParameter();
            parameter.ParameterName = "@replacement";
            parameter.Value = replacement;
            command.Parameters.Add(parameter);

            await command.ExecuteNonQueryAsync(cancellationToken);
        }
        finally
        {
            if (shouldClose)
                await connection.CloseAsync();
        }
    }

    private static async Task EnsureIndexAsync(
        MappingFeedDbContext db,
        string indexName,
        string tableName,
        string columnName,
        CancellationToken cancellationToken)
    {
        var connection = db.Database.GetDbConnection();
        var shouldClose = connection.State != ConnectionState.Open;

        if (shouldClose)
            await connection.OpenAsync(cancellationToken);

        try
        {
            await using var command = connection.CreateCommand();
            command.CommandText = $"CREATE INDEX IF NOT EXISTS {indexName} ON {tableName} ({columnName});";
            await command.ExecuteNonQueryAsync(cancellationToken);
        }
        finally
        {
            if (shouldClose)
                await connection.CloseAsync();
        }
    }
}
