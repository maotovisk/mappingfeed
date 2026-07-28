using MappingFeed.Common.Data.Entities;
using MappingFeed.Common.Data.Enums;

namespace MappingFeed.Common.Repositories.SubscribedFeed;

public interface ISubscribedFeedRepository
{
    Task<IReadOnlyList<SubscribedChannel>> GetByFeedTypeAsync(FeedType feedType, CancellationToken cancellationToken);

    Task<IReadOnlyList<SubscribedChannel>> GetByChannelAsync(long channelId, CancellationToken cancellationToken);

    Task<SubscribedChannel?> GetAsync(long channelId, FeedType feedType, CancellationToken cancellationToken);

    Task UpsertAsync(SubscribedChannel subscription, CancellationToken cancellationToken);

    Task<bool> DeleteAsync(long channelId, FeedType feedType, CancellationToken cancellationToken);

    Task AdvanceCursorAsync(SubscribedChannel subscription, long eventId, CancellationToken cancellationToken);
}
