using MappingFeed.Common.Data.Entities;
using MappingFeed.Common.Data.Enums;
using MappingFeed.Common.Services.SubscribedFeed;

namespace MappingFeed.Discord.Dispatchers.Events;

public sealed class FeedEventsDispatcher(
    ISubscribedFeedService subscribedFeedService,
    BeatmapEventsDispatcher beatmapEventsDispatcher,
    GroupEventsDispatcher groupEventsDispatcher)
{
    public async Task DispatchAsync(CancellationToken cancellationToken)
    {
        await DispatchByFeedTypeAsync(FeedType.Map, cancellationToken);
        await DispatchByFeedTypeAsync(FeedType.Group, cancellationToken);
    }

    private async Task DispatchByFeedTypeAsync(FeedType feedType, CancellationToken cancellationToken)
    {
        var subscriptions = await subscribedFeedService.GetSubscriptionsAsync(feedType, cancellationToken);

        foreach (var subscription in subscriptions)
            await DispatchSubscriptionAsync(subscription, cancellationToken);
    }

    private Task DispatchSubscriptionAsync(
        SubscribedChannel subscription,
        CancellationToken cancellationToken)
    {
        return subscription.FeedType switch
        {
            FeedType.Map => beatmapEventsDispatcher.DispatchAsync(subscription, cancellationToken),
            FeedType.Group => groupEventsDispatcher.DispatchAsync(subscription, cancellationToken),
            _ => Task.CompletedTask,
        };
    }
}
