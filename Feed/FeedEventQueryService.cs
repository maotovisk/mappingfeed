using MappingFeed.Data;
using MappingFeed.Data.Entities;
using Microsoft.EntityFrameworkCore;

namespace MappingFeed.Feed;

public sealed class FeedEventQueryService(
    IDbContextFactory<MappingFeedDbContext> dbContextFactory,
    FeedEventViewFactory viewFactory)
{
    private const int DefaultLimit = 20;
    private const int MaxLimit = 100;

    public async Task<FeedCursorPage<FeedEventViewEntry>> GetRecentMapEventsPageAsync(
        int? limit,
        long? beforeEventId,
        CancellationToken cancellationToken)
    {
        return await GetRecentEventsPageAsync(
            limit,
            beforeEventId,
            QueryMapEventsAsync,
            (entry, ct) => viewFactory.CreateBeatmapsetEventEntryAsync(entry, ct),
            x => x.EventId,
            cancellationToken);
    }

    public async Task<FeedCursorPage<FeedEventViewEntry>> GetRecentGroupEventsPageAsync(
        int? limit,
        long? beforeEventId,
        CancellationToken cancellationToken)
    {
        return await GetRecentEventsPageAsync(
            limit,
            beforeEventId,
            QueryGroupEventsAsync,
            (entry, ct) => viewFactory.CreateGroupEventEntryAsync(entry, ct),
            x => x.EventId,
            cancellationToken);
    }

    private async Task<FeedCursorPage<FeedEventViewEntry>> GetRecentEventsPageAsync<TEntry>(
        int? limit,
        long? beforeEventId,
        Func<MappingFeedDbContext, int, long?, CancellationToken, Task<List<TEntry>>> queryRows,
        Func<TEntry, CancellationToken, Task<FeedEventViewEntry>> buildEntry,
        Func<TEntry, long> getEventId,
        CancellationToken cancellationToken)
    {
        var take = NormalizeLimit(limit);

        await using var db = await dbContextFactory.CreateDbContextAsync(cancellationToken);
        var rows = await queryRows(db, take + 1, beforeEventId, cancellationToken);

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
        CancellationToken cancellationToken)
    {
        var query = db.BeatmapsetEvents.AsNoTracking();
        if (beforeEventId is not null)
            query = query.Where(x => x.EventId < beforeEventId.Value);

        return await query
            .OrderByDescending(x => x.EventId)
            .Take(take)
            .ToListAsync(cancellationToken);
    }

    private static async Task<List<GroupEvent>> QueryGroupEventsAsync(
        MappingFeedDbContext db,
        int take,
        long? beforeEventId,
        CancellationToken cancellationToken)
    {
        var query = db.GroupEvents.AsNoTracking();
        if (beforeEventId is not null)
            query = query.Where(x => x.EventId < beforeEventId.Value);

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
