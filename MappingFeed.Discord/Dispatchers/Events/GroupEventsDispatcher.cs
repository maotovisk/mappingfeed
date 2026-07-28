using MappingFeed.Common.Config;
using MappingFeed.Common.Data.Entities;
using MappingFeed.Common.Data.Enums;
using MappingFeed.Common.Services.Group;
using MappingFeed.Common.Services.SubscribedFeed;
using MappingFeed.Discord.Events.EmbedFactories;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NetCord;
using NetCord.Rest;

namespace MappingFeed.Discord.Dispatchers.Events;

public sealed class GroupEventsDispatcher(
    IGroupEventService groupEventService,
    ISubscribedFeedService subscribedFeedService,
    FeedEmbedFactory embedFactory,
    IOptions<FeedOptions> options,
    RestClient restClient,
    ILogger<GroupEventsDispatcher> logger)
{
    private const int MaxDispatchBatchSize = 10;
    private const int MinDispatchIntervalSeconds = 180;

    private readonly FeedOptions _options = options.Value;

    public async Task DispatchAsync(
        SubscribedChannel subscription,
        CancellationToken cancellationToken)
    {
        await DispatchGroupEventsAsync(subscription, cancellationToken);
    }

    private async Task DispatchGroupEventsAsync(
        SubscribedChannel subscription,
        CancellationToken cancellationToken)
    {
        var channel = await GetTextChannelAsync(subscription.ChannelId, cancellationToken);
        if (channel is null)
            return;

        var pendingEvents = await groupEventService.GetPendingEventsAsync(
            subscription.LastEventId,
            GetDispatchBatchSize(),
            cancellationToken);
        var allowedGroupIds = FeedEnumExtensions.DeserializeGroupIds(subscription.GroupId);

        foreach (var pendingEvent in pendingEvents)
        {
            if (pendingEvent.EventType == FeedEventType.GroupMove)
            {
                await subscribedFeedService.AdvanceCursorAsync(subscription, pendingEvent.EventId, cancellationToken);
                continue;
            }

            if (allowedGroupIds is not null && !allowedGroupIds.Contains(pendingEvent.GroupId))
            {
                await subscribedFeedService.AdvanceCursorAsync(subscription, pendingEvent.EventId, cancellationToken);
                continue;
            }

            try
            {
                var embed = await embedFactory.CreateGroupEventEmbedAsync(pendingEvent, cancellationToken);
                var userUrl = $"https://osu.ppy.sh/users/{pendingEvent.UserId}";

                await channel.SendMessageAsync(
                    new MessageProperties()
                        .WithContent(userUrl)
                        .WithEmbeds([embed]),
                    cancellationToken: cancellationToken);

                await subscribedFeedService.AdvanceCursorAsync(subscription, pendingEvent.EventId, cancellationToken);
            }
            catch (Exception exception)
            {
                logger.LogError(
                    exception,
                    "Failed sending group event {EventId} to channel {ChannelId}.",
                    pendingEvent.EventId,
                    subscription.ChannelId);
                break;
            }
        }
    }

    private async Task<TextChannel?> GetTextChannelAsync(long channelId, CancellationToken cancellationToken)
    {
        try
        {
            var channel = await restClient.GetChannelAsync((ulong)channelId, cancellationToken: cancellationToken);

            if (channel is TextChannel textChannel)
                return textChannel;

            logger.LogWarning("Channel {ChannelId} is not a text channel.", channelId);
            return null;
        }
        catch (Exception exception)
        {
            logger.LogError(exception, "Failed to fetch channel {ChannelId}.", channelId);
            return null;
        }
    }

    private int GetDispatchBatchSize()
    {
        return Math.Clamp(_options.DispatchBatchSize, 1, MaxDispatchBatchSize);
    }

    private int GetDispatchIntervalSeconds()
    {
        return Math.Max(_options.DispatchIntervalSeconds, MinDispatchIntervalSeconds);
    }

}
