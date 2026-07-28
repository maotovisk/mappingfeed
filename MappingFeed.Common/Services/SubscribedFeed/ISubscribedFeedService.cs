using MappingFeed.Common.Data.Entities;
using MappingFeed.Common.Data.Enums;

namespace MappingFeed.Common.Services.SubscribedFeed;

public interface ISubscribedFeedService
{
    Task<IReadOnlyList<SubscribedChannel>> GetSubscriptionsAsync(FeedType feedType, CancellationToken cancellationToken);

    Task AdvanceCursorAsync(SubscribedChannel subscription, long eventId, CancellationToken cancellationToken);

    Task<IReadOnlyList<SubscribedChannel>> GetChannelSubscriptionsAsync(long channelId, CancellationToken cancellationToken = default);

    Task<SubscribedChannel?> GetSubscriptionAsync(long channelId, FeedType feedType, CancellationToken cancellationToken = default);

    Task<string> UpsertSubscriptionAsync(
        long channelId,
        FeedType feedType,
        HashSet<Ruleset>? rulesets,
        HashSet<FeedEventType>? eventTypes,
        HashSet<long>? groupIds,
        DateTimeOffset? startCursorSince = null,
        CancellationToken cancellationToken = default);

    Task<bool> DeleteSubscriptionAsync(long channelId, FeedType feedType, CancellationToken cancellationToken = default);
}
