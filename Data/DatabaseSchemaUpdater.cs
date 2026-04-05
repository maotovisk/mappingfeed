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
        await EnsureColumnAsync(db, "group_events", "raw_event", "TEXT", cancellationToken);
        await BackfillNullTextColumnAsync(db, "group_events", "raw_event", "{}", cancellationToken);
        await BackfillNullTextColumnAsync(db, "beatmapset_events", "raw_event", "{}", cancellationToken);
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
}
