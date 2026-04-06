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
        var take = NormalizeLimit(limit);

        await using var db = await dbContextFactory.CreateDbContextAsync(cancellationToken);
        var rows = await QueryMapEventsAsync(db, take + 1, beforeEventId, cancellationToken);

        var hasMore = rows.Count > take;
        var pageRows = rows.Take(take).ToList();
        var items = await Task.WhenAll(pageRows.Select(x => viewFactory.CreateBeatmapsetEventEntryAsync(x, cancellationToken)));

        var nextCursor = hasMore && pageRows.Count > 0
            ? pageRows[^1].EventId.ToString()
            : null;

        return new FeedCursorPage<FeedEventViewEntry>(items, nextCursor);
    }

    public async Task<FeedCursorPage<FeedEventViewEntry>> GetRecentGroupEventsPageAsync(
        int? limit,
        long? beforeEventId,
        CancellationToken cancellationToken)
    {
        var take = NormalizeLimit(limit);

        await using var db = await dbContextFactory.CreateDbContextAsync(cancellationToken);
        var rows = await QueryGroupEventsAsync(db, take + 1, beforeEventId, cancellationToken);

        var hasMore = rows.Count > take;
        var pageRows = rows.Take(take).ToList();
        var items = await Task.WhenAll(pageRows.Select(x => viewFactory.CreateGroupEventEntryAsync(x, cancellationToken)));

        var nextCursor = hasMore && pageRows.Count > 0
            ? pageRows[^1].EventId.ToString()
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
