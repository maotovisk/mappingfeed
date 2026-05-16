using MappingFeed.Data;
using MappingFeed.Data.Entities;
using Microsoft.EntityFrameworkCore;

namespace MappingFeed.Repositories.Group;

public sealed class GroupEventRepository(IDbContextFactory<MappingFeedDbContext> dbContextFactory) : IGroupEventRepository
{
    public async Task<HashSet<long>> GetExistingEventIdsAsync(
        IReadOnlyCollection<long> eventIds,
        CancellationToken cancellationToken)
    {
        await using var db = await dbContextFactory.CreateDbContextAsync(cancellationToken);

        return await db.GroupEvents
            .Where(x => eventIds.Contains(x.EventId))
            .Select(x => x.EventId)
            .ToHashSetAsync(cancellationToken);
    }

    public async Task AddRangeAsync(
        IReadOnlyCollection<GroupEvent> events,
        CancellationToken cancellationToken)
    {
        if (events.Count == 0)
            return;

        await using var db = await dbContextFactory.CreateDbContextAsync(cancellationToken);
        db.GroupEvents.AddRange(events);
        await db.SaveChangesAsync(cancellationToken);
    }

    public async Task<IReadOnlyList<GroupEvent>> GetPendingEventsAsync(
        long afterEventId,
        int take,
        CancellationToken cancellationToken)
    {
        await using var db = await dbContextFactory.CreateDbContextAsync(cancellationToken);

        return await db.GroupEvents
            .Where(x => x.EventId > afterEventId)
            .OrderBy(x => x.EventId)
            .Take(take)
            .ToListAsync(cancellationToken);
    }

    public async Task<List<GroupEvent>> QueryRecentAsync(
        int take,
        long? beforeEventId,
        GroupEventsFilter filters,
        CancellationToken cancellationToken)
    {
        await using var db = await dbContextFactory.CreateDbContextAsync(cancellationToken);

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
}
