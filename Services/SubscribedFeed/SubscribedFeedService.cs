using MappingFeed.Data.Entities;
using MappingFeed.Repositories.SubscribedFeed;

namespace MappingFeed.Services.SubscribedFeed;

public sealed class SubscribedFeedService(ISubscribedFeedRepository repository) : ISubscribedFeedService
{
    public Task<IReadOnlyList<SubscribedChannel>> GetSubscriptionsAsync(
        FeedType feedType,
        CancellationToken cancellationToken)
    {
        return repository.GetByFeedTypeAsync(feedType, cancellationToken);
    }

    public Task AdvanceCursorAsync(
        SubscribedChannel subscription,
        long eventId,
        CancellationToken cancellationToken)
    {
        return repository.AdvanceCursorAsync(subscription, eventId, cancellationToken);
    }

    public Task<IReadOnlyList<SubscribedChannel>> GetChannelSubscriptionsAsync(
        long channelId,
        CancellationToken cancellationToken = default)
    {
        return repository.GetByChannelAsync(channelId, cancellationToken);
    }

    public Task<SubscribedChannel?> GetSubscriptionAsync(
        long channelId,
        FeedType feedType,
        CancellationToken cancellationToken = default)
    {
        return repository.GetAsync(channelId, feedType, cancellationToken);
    }

    public async Task<string> UpsertSubscriptionAsync(
        long channelId,
        FeedType feedType,
        HashSet<Ruleset>? rulesets,
        HashSet<FeedEventType>? eventTypes,
        HashSet<long>? groupIds,
        CancellationToken cancellationToken = default)
    {
        var serializedRulesets = FeedEnumExtensions.SerializeRulesets(rulesets);
        var serializedEventTypes = FeedEnumExtensions.SerializeEventTypes(eventTypes);
        var serializedGroupIds = FeedEnumExtensions.SerializeGroupIds(groupIds);
        var existingSubscription = await repository.GetAsync(channelId, feedType, cancellationToken);

        if (existingSubscription is not null)
        {
            var existingSerializedGroupIds = FeedEnumExtensions.SerializeGroupIds(
                FeedEnumExtensions.DeserializeGroupIds(existingSubscription.GroupId));

            if (string.Equals(existingSubscription.Rulesets, serializedRulesets, StringComparison.Ordinal) &&
                string.Equals(existingSubscription.EventTypes, serializedEventTypes, StringComparison.Ordinal) &&
                string.Equals(existingSerializedGroupIds, serializedGroupIds, StringComparison.Ordinal))
                return $"This channel is already subscribed to `{feedType.ToCommandValue()}` ({BuildFilterSummary(feedType, existingSubscription.Rulesets, existingSubscription.EventTypes, existingSerializedGroupIds)}).";
        }

        await repository.UpsertAsync(
            new SubscribedChannel
            {
                ChannelId = channelId,
                FeedType = feedType,
                LastEventId = existingSubscription?.LastEventId ?? 0,
                Rulesets = serializedRulesets,
                EventTypes = serializedEventTypes,
                GroupId = serializedGroupIds,
            },
            cancellationToken);

        return existingSubscription is null
            ? $"Subscribed this channel to `{feedType.ToCommandValue()}` ({BuildFilterSummary(feedType, serializedRulesets, serializedEventTypes, serializedGroupIds)})."
            : $"Updated `{feedType.ToCommandValue()}` subscription ({BuildFilterSummary(feedType, serializedRulesets, serializedEventTypes, serializedGroupIds)}).";
    }

    public Task<bool> DeleteSubscriptionAsync(
        long channelId,
        FeedType feedType,
        CancellationToken cancellationToken = default)
    {
        return repository.DeleteAsync(channelId, feedType, cancellationToken);
    }

    public static string BuildFilterSummary(
        FeedType feedType,
        string? serializedRulesets,
        string? serializedEventTypes,
        string? serializedGroupIds)
    {
        return feedType switch
        {
            FeedType.Map =>
                $"rulesets: {FeedEnumExtensions.FormatRulesetsForDisplay(serializedRulesets)}, event types: {FeedEnumExtensions.FormatEventTypesForDisplay(serializedEventTypes)}",
            FeedType.Group =>
                $"group ids: {FeedEnumExtensions.FormatGroupIdsForDisplay(serializedGroupIds)}",
            _ => "default",
        };
    }
}
