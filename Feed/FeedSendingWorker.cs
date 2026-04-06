using MappingFeed.Config;
using MappingFeed.Data;
using MappingFeed.Data.Entities;
using MappingFeed.Osu;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;
using NetCord;
using NetCord.Rest;
using System.Globalization;
using System.Text.Json.Nodes;

namespace MappingFeed.Feed;

public sealed class FeedSendingWorker(
    IDbContextFactory<MappingFeedDbContext> dbContextFactory,
    FeedEmbedFactory embedFactory,
    IOptions<FeedOptions> options,
    RestClient restClient,
    ILogger<FeedSendingWorker> logger) : BackgroundService
{
    private const int MaxDispatchBatchSize = 10;
    private const int MinDispatchIntervalSeconds = 180;

    private readonly FeedOptions _options = options.Value;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await DispatchByFeedTypeAsync(FeedType.Map, stoppingToken);
                await DispatchByFeedTypeAsync(FeedType.Group, stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception exception)
            {
                logger.LogError(exception, "Failed while sending feed events.");
            }

            await Task.Delay(TimeSpan.FromSeconds(GetDispatchIntervalSeconds()), stoppingToken);
        }
    }

    private async Task DispatchByFeedTypeAsync(FeedType feedType, CancellationToken cancellationToken)
    {
        await using var db = await dbContextFactory.CreateDbContextAsync(cancellationToken);

        var subscriptions = await db.SubscribedChannels
            .Where(x => x.FeedType == feedType)
            .OrderBy(x => x.ChannelId)
            .ToListAsync(cancellationToken);

        foreach (var subscription in subscriptions)
        {
            if (feedType == FeedType.Map)
                await DispatchBeatmapsetEventsAsync(db, subscription, cancellationToken);
            else
                await DispatchGroupEventsAsync(db, subscription, cancellationToken);
        }
    }

    private async Task DispatchBeatmapsetEventsAsync(
        MappingFeedDbContext db,
        SubscribedChannel subscription,
        CancellationToken cancellationToken)
    {
        var channel = await GetTextChannelAsync(subscription.ChannelId, cancellationToken);
        if (channel is null)
            return;

        var pendingEvents = await db.BeatmapsetEvents
            .Where(x => x.EventId > subscription.LastEventId)
            .OrderBy(x => x.EventId)
            .Take(GetDispatchBatchSize())
            .ToListAsync(cancellationToken);
        var allowedRulesets = FeedEnumExtensions.DeserializeRulesets(subscription.Rulesets);
        var allowedEventTypes = FeedEnumExtensions.DeserializeEventTypes(subscription.EventTypes);
        var mergePlan = await BuildMapMergePlanAsync(db, pendingEvents, cancellationToken);

        foreach (var pendingEvent in pendingEvents)
        {
            var hasMergedNomination = mergePlan.NominationForQualification.TryGetValue(pendingEvent.EventId, out var mergedNominationValue);
            var mergedNomination = hasMergedNomination ? mergedNominationValue : null;

            if (mergePlan.NominationEventIdsToSuppress.Contains(pendingEvent.EventId))
            {
                await AdvanceSubscriptionCursorAsync(db, subscription, pendingEvent.EventId, cancellationToken);
                continue;
            }

            if (pendingEvent.EventType == FeedEventType.Qualification &&
                hasMergedNomination)
            {
                var shouldSave = false;

                if (pendingEvent.TriggeredBy is null && mergedNomination!.TriggeredBy is not null)
                {
                    pendingEvent.TriggeredBy = mergedNomination.TriggeredBy;
                    shouldSave = true;
                }

                if (string.IsNullOrWhiteSpace(pendingEvent.Message) && !string.IsNullOrWhiteSpace(mergedNomination!.Message))
                {
                    pendingEvent.Message = mergedNomination.Message;
                    shouldSave = true;
                }

                if (shouldSave)
                    await db.SaveChangesAsync(cancellationToken);
            }

            var rawEventForRuleset = pendingEvent.RawEvent;
            var serializedRulesetsForDispatch = pendingEvent.Rulesets;
            if (pendingEvent.EventType == FeedEventType.Qualification &&
                hasMergedNomination &&
                FeedEnumExtensions.ExtractRulesets(rawEventForRuleset, pendingEvent.Rulesets).Count == 0)
            {
                rawEventForRuleset = mergedNomination!.RawEvent;
                serializedRulesetsForDispatch = mergedNomination.Rulesets;
            }

            if (!ShouldDispatchToRulesets(
                    allowedRulesets,
                    serializedRulesetsForDispatch,
                    rawEventForRuleset))
            {
                await AdvanceSubscriptionCursorAsync(db, subscription, pendingEvent.EventId, cancellationToken);
                continue;
            }

            if (allowedEventTypes is not null &&
                allowedEventTypes.Count > 0 &&
                !allowedEventTypes.Contains(pendingEvent.EventType))
            {
                await AdvanceSubscriptionCursorAsync(db, subscription, pendingEvent.EventId, cancellationToken);
                continue;
            }

            try
            {
                var embed = await embedFactory.CreateBeatmapsetEventEmbedAsync(pendingEvent, cancellationToken);
                var beatmapsetUrl = $"https://osu.ppy.sh/beatmapsets/{pendingEvent.SetId}";

                await channel.SendMessageAsync(
                    new MessageProperties()
                        .WithContent(beatmapsetUrl)
                        .WithEmbeds([embed]),
                    cancellationToken: cancellationToken);

                await AdvanceSubscriptionCursorAsync(db, subscription, pendingEvent.EventId, cancellationToken);
            }
            catch (Exception exception)
            {
                logger.LogError(
                    exception,
                    "Failed sending beatmapset event {EventId} to channel {ChannelId}.",
                    pendingEvent.EventId,
                    subscription.ChannelId);
                break;
            }
        }
    }

    private static bool ShouldDispatchToRulesets(
        HashSet<Ruleset>? allowedRulesets,
        string? serializedRulesets,
        string rawEvent)
    {
        if (allowedRulesets is null || allowedRulesets.Count == 0)
            return true;

        var eventRulesets = FeedEnumExtensions.DeserializeRulesets(serializedRulesets)
            ?? FeedEnumExtensions.ExtractRulesets(rawEvent);
        return eventRulesets.Count == 0 || eventRulesets.Overlaps(allowedRulesets);
    }

    private async Task<MapMergePlan> BuildMapMergePlanAsync(
        MappingFeedDbContext db,
        IReadOnlyCollection<BeatmapsetEvent> pendingEvents,
        CancellationToken cancellationToken)
    {
        var eventInfos = pendingEvents
            .Select(x => new MapEventInfo(x, x.CreatedAt ?? TryGetCreatedAt(x.RawEvent)))
            .ToList();

        var nominationEventIdsToSuppress = new HashSet<long>();
        var nominationForQualification = new Dictionary<long, BeatmapsetEvent>();

        foreach (var qualification in eventInfos
                     .Where(x => x.Event.EventType == FeedEventType.Qualification)
                     .OrderBy(x => x.Event.EventId))
        {
            if (qualification.CreatedAt is null)
                continue;

            var nominationsToThisQualification = eventInfos
                .Where(x =>
                    x.Event.EventType == FeedEventType.Nomination &&
                    x.Event.SetId == qualification.Event.SetId &&
                    x.Event.EventId < qualification.Event.EventId &&
                    x.CreatedAt is not null &&
                    qualification.CreatedAt >= x.CreatedAt &&
                    qualification.CreatedAt - x.CreatedAt <= TimeSpan.FromSeconds(45))
                .OrderByDescending(x => x.CreatedAt)
                .ThenByDescending(x => x.Event.EventId)
                .ToList();

            if (nominationsToThisQualification.Count == 0)
                continue;

            var nominationCandidate = nominationsToThisQualification[0];
            var hasEarlierNominationInPending = eventInfos.Any(x =>
                x.Event.EventType == FeedEventType.Nomination &&
                x.Event.SetId == qualification.Event.SetId &&
                x.Event.EventId < nominationCandidate.Event.EventId);

            var hasEarlierNominationInDb = hasEarlierNominationInPending || await db.BeatmapsetEvents
                .AnyAsync(
                    x => x.EventType == FeedEventType.Nomination
                         && x.SetId == qualification.Event.SetId
                         && x.EventId < nominationCandidate.Event.EventId,
                    cancellationToken);

            if (!hasEarlierNominationInDb)
                continue;

            nominationEventIdsToSuppress.Add(nominationCandidate.Event.EventId);
            nominationForQualification[qualification.Event.EventId] = nominationCandidate.Event;
        }

        return new MapMergePlan(nominationEventIdsToSuppress, nominationForQualification);
    }

    private static DateTimeOffset? TryGetCreatedAt(string rawEvent)
    {
        try
        {
            var root = JsonNode.Parse(rawEvent) as JsonObject;
            var createdAtRaw = root?.TryGetString("created_at");
            if (createdAtRaw is null)
                return null;

            if (DateTimeOffset.TryParse(createdAtRaw, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal, out var createdAt))
                return createdAt;
        }
        catch
        {
            // Ignore malformed payload.
        }

        return null;
    }

    private async Task DispatchGroupEventsAsync(
        MappingFeedDbContext db,
        SubscribedChannel subscription,
        CancellationToken cancellationToken)
    {
        var channel = await GetTextChannelAsync(subscription.ChannelId, cancellationToken);
        if (channel is null)
            return;

        var pendingEvents = await db.GroupEvents
            .Where(x => x.EventId > subscription.LastEventId)
            .OrderBy(x => x.EventId)
            .Take(GetDispatchBatchSize())
            .ToListAsync(cancellationToken);
        var allowedGroupIds = FeedEnumExtensions.DeserializeGroupIds(subscription.GroupId);

        foreach (var pendingEvent in pendingEvents)
        {
            if (pendingEvent.EventType == FeedEventType.GroupMove)
            {
                await AdvanceSubscriptionCursorAsync(db, subscription, pendingEvent.EventId, cancellationToken);
                continue;
            }

            if (allowedGroupIds is not null && !allowedGroupIds.Contains(pendingEvent.GroupId))
            {
                await AdvanceSubscriptionCursorAsync(db, subscription, pendingEvent.EventId, cancellationToken);
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

                await AdvanceSubscriptionCursorAsync(db, subscription, pendingEvent.EventId, cancellationToken);
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

    private static async Task AdvanceSubscriptionCursorAsync(
        MappingFeedDbContext db,
        SubscribedChannel subscription,
        long eventId,
        CancellationToken cancellationToken)
    {
        subscription.LastEventId = eventId;
        await db.SaveChangesAsync(cancellationToken);
    }

    private sealed record MapEventInfo(BeatmapsetEvent Event, DateTimeOffset? CreatedAt);

    private sealed record MapMergePlan(
        HashSet<long> NominationEventIdsToSuppress,
        Dictionary<long, BeatmapsetEvent> NominationForQualification);
}
