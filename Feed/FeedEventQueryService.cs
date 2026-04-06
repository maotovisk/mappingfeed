using MappingFeed.Data;
using MappingFeed.Data.Entities;
using Microsoft.EntityFrameworkCore;

namespace MappingFeed.Feed;

public sealed record MapEventsFilter(
    Ruleset? Ruleset,
    IReadOnlyCollection<FeedEventType>? EventTypes,
    string? Text);

public sealed record GroupEventsFilter(
    IReadOnlyCollection<long>? GroupIds,
    string? Playmode);

public sealed class FeedEventQueryService(
    IDbContextFactory<MappingFeedDbContext> dbContextFactory,
    FeedEventViewFactory viewFactory)
{
    private const int DefaultLimit = 20;
    private const int MaxLimit = 100;

    public async Task<FeedCursorPage<FeedEventViewEntry>> GetRecentMapEventsPageAsync(
        int? limit,
        long? beforeEventId,
        MapEventsFilter filters,
        CancellationToken cancellationToken)
    {
        return await GetRecentEventsPageAsync(
            limit,
            beforeEventId,
            filters,
            QueryMapEventsAsync,
            (entry, ct) => viewFactory.CreateBeatmapsetEventEntryAsync(entry, ct),
            x => x.EventId,
            cancellationToken);
    }

    public async Task<FeedCursorPage<FeedEventViewEntry>> GetRecentGroupEventsPageAsync(
        int? limit,
        long? beforeEventId,
        GroupEventsFilter filters,
        CancellationToken cancellationToken)
    {
        return await GetRecentEventsPageAsync(
            limit,
            beforeEventId,
            filters,
            QueryGroupEventsAsync,
            viewFactory.CreateGroupEventEntryAsync,
            x => x.EventId,
            cancellationToken);
    }

    private async Task<FeedCursorPage<FeedEventViewEntry>> GetRecentEventsPageAsync<TEntry, TFilters>(
        int? limit,
        long? beforeEventId,
        TFilters filters,
        Func<MappingFeedDbContext, int, long?, TFilters, CancellationToken, Task<List<TEntry>>> queryRows,
        Func<TEntry, CancellationToken, Task<FeedEventViewEntry>> buildEntry,
        Func<TEntry, long> getEventId,
        CancellationToken cancellationToken)
    {
        var take = NormalizeLimit(limit);

        await using var db = await dbContextFactory.CreateDbContextAsync(cancellationToken);
        var rows = await queryRows(db, take + 1, beforeEventId, filters, cancellationToken);

        var hasMore = rows.Count > take;
        var pageRows = rows.Take(take).ToList();
        var items = await Task.WhenAll(pageRows.Select(x => buildEntry(x, cancellationToken)));

        var nextCursor = hasMore && pageRows.Count > 0
            ? getEventId(pageRows[^1]).ToString()
            : null;

        return new FeedCursorPage<FeedEventViewEntry>(items, nextCursor);
    }

    private static async Task<List<BeatmapsetEvent>> QueryMapEventsAsync(
        MappingFeedDbContext db,
        int take,
        long? beforeEventId,
        MapEventsFilter filters,
        CancellationToken cancellationToken)
    {
        var query = db.BeatmapsetEvents.AsNoTracking();
        if (beforeEventId is not null)
            query = query.Where(x => x.EventId < beforeEventId.Value);

        if (filters.EventTypes is { Count: > 0 })
        {
            var eventTypes = filters.EventTypes.ToArray();
            query = query.Where(x => eventTypes.Contains(x.EventType));
        }

        if (filters.Ruleset is not null)
        {
            var rulesetToken = filters.Ruleset.Value.ToCommandValue();
            var rulesetMatchToken = $"|{rulesetToken}|";
            query = query.Where(x =>
                x.Rulesets != null &&
                ($"|" + x.Rulesets + "|").Contains(rulesetMatchToken));
        }

        if (!string.IsNullOrWhiteSpace(filters.Text))
        {
            var text = filters.Text.Trim();
            var loweredText = text.ToLowerInvariant();
            var hasNumericText = long.TryParse(text, out var parsedNumber);

            query = query.Where(x =>
                (x.BeatmapsetTitle != null && x.BeatmapsetTitle.ToLower().Contains(loweredText)) ||
                (x.MapperName != null && x.MapperName.ToLower().Contains(loweredText)) ||
                (x.ActorUsername != null && x.ActorUsername.ToLower().Contains(loweredText)) ||
                (x.Message != null && x.Message.ToLower().Contains(loweredText)) ||
                (hasNumericText && (
                    x.SetId == parsedNumber ||
                    x.EventId == parsedNumber ||
                    x.TriggeredBy == parsedNumber ||
                    x.MapperUserId == parsedNumber)));
        }

        return await query
            .OrderByDescending(x => x.EventId)
            .Take(take)
            .ToListAsync(cancellationToken);
    }

    private static async Task<List<GroupEvent>> QueryGroupEventsAsync(
        MappingFeedDbContext db,
        int take,
        long? beforeEventId,
        GroupEventsFilter filters,
        CancellationToken cancellationToken)
    {
        var query = db.GroupEvents.AsNoTracking();
        if (beforeEventId is not null)
            query = query.Where(x => x.EventId < beforeEventId.Value);

        if (filters.GroupIds is { Count: > 0 })
        {
            var groupIds = filters.GroupIds.ToArray();
            query = query.Where(x => groupIds.Contains(x.GroupId));
        }

        if (!string.IsNullOrWhiteSpace(filters.Playmode))
        {
            var playmode = filters.Playmode.Trim().ToLowerInvariant();
            var playmodeMatchToken = $",{playmode},";
            query = query.Where(x =>
                x.Playmodes != null &&
                ("," + x.Playmodes.Replace(" ", "").ToLower() + ",").Contains(playmodeMatchToken));
        }

        return await query
            .OrderByDescending(x => x.EventId)
            .Take(take)
            .ToListAsync(cancellationToken);
    }

    private static int NormalizeLimit(int? limit)
    {
        if (limit is null)
            return DefaultLimit;

        return Math.Clamp(limit.Value, 1, MaxLimit);
    }
}
