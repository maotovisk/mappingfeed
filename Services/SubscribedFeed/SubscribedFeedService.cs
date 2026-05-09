using MappingFeed.Data;
using MappingFeed.Data.Entities;
using MappingFeed.Repositories.SubscribedFeed;
using Microsoft.EntityFrameworkCore;

namespace MappingFeed.Services.SubscribedFeed;

public sealed class SubscribedFeedService(
    ISubscribedFeedRepository repository,
    IDbContextFactory<MappingFeedDbContext> dbContextFactory)
    : ISubscribedFeedService
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
        DateTimeOffset? startCursorSince = null,
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

            if (startCursorSince is null &&
                string.Equals(existingSubscription.Rulesets, serializedRulesets, StringComparison.Ordinal) &&
                string.Equals(existingSubscription.EventTypes, serializedEventTypes, StringComparison.Ordinal) &&
                string.Equals(existingSerializedGroupIds, serializedGroupIds, StringComparison.Ordinal))
                return $"This channel is already subscribed to `{feedType.ToCommandValue()}` ({BuildFilterSummary(feedType, existingSubscription.Rulesets, existingSubscription.EventTypes, existingSerializedGroupIds)}).";
        }

        var lastEventId = startCursorSince is null
            ? existingSubscription?.LastEventId ?? 0
            : await ResolveInitialLastEventIdAsync(feedType, startCursorSince.Value, cancellationToken);

        await repository.UpsertAsync(
            new SubscribedChannel
            {
                ChannelId = channelId,
                FeedType = feedType,
                LastEventId = lastEventId,
                Rulesets = serializedRulesets,
                EventTypes = serializedEventTypes,
                GroupId = serializedGroupIds,
            },
            cancellationToken);

        return existingSubscription is null
            ? $"Subscribed this channel to `{feedType.ToCommandValue()}` ({BuildFilterSummary(feedType, serializedRulesets, serializedEventTypes, serializedGroupIds)})."
            : $"Updated `{feedType.ToCommandValue()}` subscription ({BuildFilterSummary(feedType, serializedRulesets, serializedEventTypes, serializedGroupIds)}).";
    }

    private async Task<long> ResolveInitialLastEventIdAsync(
        FeedType feedType,
        DateTimeOffset since,
        CancellationToken cancellationToken)
    {
        await using var db = await dbContextFactory.CreateDbContextAsync(cancellationToken);

        var firstEventIdInWindow = await GetFirstEventIdInWindowAsync(db, feedType, since, cancellationToken);

        if (firstEventIdInWindow is not null)
            return Math.Max(0, firstEventIdInWindow.Value - 1);

        return feedType switch
        {
            FeedType.Map => await db.BeatmapsetEvents
                .AsNoTracking()
                .OrderByDescending(x => x.EventId)
                .Select(x => (long?)x.EventId)
                .FirstOrDefaultAsync(cancellationToken) ?? 0,
            FeedType.Group => await db.GroupEvents
                .AsNoTracking()
                .OrderByDescending(x => x.EventId)
                .Select(x => (long?)x.EventId)
                .FirstOrDefaultAsync(cancellationToken) ?? 0,
            _ => 0,
        };
    }

    private static async Task<long?> GetFirstEventIdInWindowAsync(
        MappingFeedDbContext db,
        FeedType feedType,
        DateTimeOffset since,
        CancellationToken cancellationToken)
    {
        switch (feedType)
        {
            case FeedType.Map:
            {
                var events = db.BeatmapsetEvents
                    .AsNoTracking()
                    .OrderByDescending(x => x.EventId)
                    .Select(x => new { x.EventId, x.CreatedAt })
                    .AsAsyncEnumerable()
                    .WithCancellation(cancellationToken);
                long? firstEventIdInWindow = null;

                await foreach (var eventInfo in events)
                {
                    if (eventInfo.CreatedAt is null)
                        continue;

                    if (eventInfo.CreatedAt >= since)
                    {
                        firstEventIdInWindow = eventInfo.EventId;
                        continue;
                    }

                    break;
                }

                return firstEventIdInWindow;
            }
            case FeedType.Group:
            {
                var events = db.GroupEvents
                    .AsNoTracking()
                    .OrderByDescending(x => x.EventId)
                    .Select(x => new { x.EventId, x.CreatedAt })
                    .AsAsyncEnumerable()
                    .WithCancellation(cancellationToken);
                long? firstEventIdInWindow = null;

                await foreach (var eventInfo in events)
                {
                    if (eventInfo.CreatedAt is null)
                        continue;

                    if (eventInfo.CreatedAt >= since)
                    {
                        firstEventIdInWindow = eventInfo.EventId;
                        continue;
                    }

                    break;
                }

                return firstEventIdInWindow;
            }
            default:
                return null;
        }
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
