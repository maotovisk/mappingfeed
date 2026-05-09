using MappingFeed.Data;
using MappingFeed.Data.Entities;
using Microsoft.EntityFrameworkCore;

namespace MappingFeed.Repositories.SubscribedFeed;

public sealed class SubscribedFeedRepository(IDbContextFactory<MappingFeedDbContext> dbContextFactory) : ISubscribedFeedRepository
{
    public async Task<IReadOnlyList<SubscribedChannel>> GetByFeedTypeAsync(
        FeedType feedType,
        CancellationToken cancellationToken)
    {
        await using var db = await dbContextFactory.CreateDbContextAsync(cancellationToken);

        return await db.SubscribedChannels
            .AsNoTracking()
            .Where(x => x.FeedType == feedType)
            .OrderBy(x => x.ChannelId)
            .ToListAsync(cancellationToken);
    }

    public async Task<IReadOnlyList<SubscribedChannel>> GetByChannelAsync(
        long channelId,
        CancellationToken cancellationToken)
    {
        await using var db = await dbContextFactory.CreateDbContextAsync(cancellationToken);

        return await db.SubscribedChannels
            .AsNoTracking()
            .Where(x => x.ChannelId == channelId)
            .OrderBy(x => x.FeedType)
            .ToListAsync(cancellationToken);
    }

    public async Task<SubscribedChannel?> GetAsync(
        long channelId,
        FeedType feedType,
        CancellationToken cancellationToken)
    {
        await using var db = await dbContextFactory.CreateDbContextAsync(cancellationToken);

        return await db.SubscribedChannels
            .AsNoTracking()
            .FirstOrDefaultAsync(
                x => x.ChannelId == channelId && x.FeedType == feedType,
                cancellationToken);
    }

    public async Task UpsertAsync(SubscribedChannel subscription, CancellationToken cancellationToken)
    {
        await using var db = await dbContextFactory.CreateDbContextAsync(cancellationToken);

        var tracked = await db.SubscribedChannels.FirstOrDefaultAsync(
            x => x.ChannelId == subscription.ChannelId && x.FeedType == subscription.FeedType,
            cancellationToken);

        if (tracked is null)
        {
            db.SubscribedChannels.Add(subscription);
        }
        else
        {
            tracked.LastEventId = subscription.LastEventId;
            tracked.Rulesets = subscription.Rulesets;
            tracked.EventTypes = subscription.EventTypes;
            tracked.GroupId = subscription.GroupId;
        }

        await db.SaveChangesAsync(cancellationToken);
    }

    public async Task<bool> DeleteAsync(
        long channelId,
        FeedType feedType,
        CancellationToken cancellationToken)
    {
        await using var db = await dbContextFactory.CreateDbContextAsync(cancellationToken);

        var tracked = await db.SubscribedChannels.FirstOrDefaultAsync(
            x => x.ChannelId == channelId && x.FeedType == feedType,
            cancellationToken);
        if (tracked is null)
            return false;

        db.SubscribedChannels.Remove(tracked);
        await db.SaveChangesAsync(cancellationToken);
        return true;
    }

    public async Task AdvanceCursorAsync(
        SubscribedChannel subscription,
        long eventId,
        CancellationToken cancellationToken)
    {
        await using var db = await dbContextFactory.CreateDbContextAsync(cancellationToken);

        var tracked = await db.SubscribedChannels.FirstOrDefaultAsync(
            x => x.ChannelId == subscription.ChannelId && x.FeedType == subscription.FeedType,
            cancellationToken);
        if (tracked is null)
            return;

        tracked.LastEventId = eventId;
        await db.SaveChangesAsync(cancellationToken);

        subscription.LastEventId = eventId;
    }
}
