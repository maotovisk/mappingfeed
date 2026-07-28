using System.Globalization;
using System.Text.Json.Nodes;
using MappingFeed.Common.Config;
using MappingFeed.Common.Data.Entities;
using MappingFeed.Common.Data.Enums;
using MappingFeed.Common.Services.Beatmap;
using MappingFeed.Common.Services.SubscribedFeed;
using MappingFeed.Common.Json;
using MappingFeed.Data.Dispatchers;
using MappingFeed.Discord.Events.EmbedFactories;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NetCord;
using NetCord.Rest;

namespace MappingFeed.Discord.Dispatchers.Events;

public sealed class BeatmapEventsDispatcher(
    IBeatmapEventService beatmapEventService,
    ISubscribedFeedService subscribedFeedService,
    FeedEmbedFactory embedFactory,
    IOptions<FeedOptions> options,
    RestClient restClient,
    ILogger<BeatmapEventsDispatcher> logger)
{
    private const int MaxDispatchBatchSize = 10;
    private const int MinDispatchIntervalSeconds = 180;

    private readonly FeedOptions _options = options.Value;

    public async Task DispatchAsync(
        SubscribedChannel subscription,
        CancellationToken cancellationToken)
    {
        var channel = await GetTextChannelAsync(subscription.ChannelId, cancellationToken);
        if (channel is null)
            return;

        var pendingEvents = await beatmapEventService.GetPendingEventsAsync(
            subscription.LastEventId,
            GetDispatchBatchSize(),
            cancellationToken);
        var allowedRulesets = FeedEnumExtensions.DeserializeRulesets(subscription.Rulesets);
        var allowedEventTypes = FeedEnumExtensions.DeserializeEventTypes(subscription.EventTypes);
        var mergePlan = await BuildMapMergePlanAsync(pendingEvents, cancellationToken);

        foreach (var pendingEvent in pendingEvents)
        {
            var hasMergedNomination = mergePlan.NominationForQualification.TryGetValue(pendingEvent.EventId, out var mergedNominationValue);
            var mergedNomination = hasMergedNomination ? mergedNominationValue : null;

            if (FeedVisibilityRules.ShouldSuppressFromPublicFeed(pendingEvent))
            {
                await subscribedFeedService.AdvanceCursorAsync(subscription, pendingEvent.EventId, cancellationToken);
                continue;
            }

            if (mergePlan.NominationEventIdsToSuppress.Contains(pendingEvent.EventId))
            {
                await subscribedFeedService.AdvanceCursorAsync(subscription, pendingEvent.EventId, cancellationToken);
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
                    await beatmapEventService.UpdateAsync(pendingEvent, cancellationToken);
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
                await subscribedFeedService.AdvanceCursorAsync(subscription, pendingEvent.EventId, cancellationToken);
                continue;
            }

            if (allowedEventTypes is not null &&
                allowedEventTypes.Count > 0 &&
                !allowedEventTypes.Contains(pendingEvent.EventType))
            {
                await subscribedFeedService.AdvanceCursorAsync(subscription, pendingEvent.EventId, cancellationToken);
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

                await subscribedFeedService.AdvanceCursorAsync(subscription, pendingEvent.EventId, cancellationToken);
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

            var hasEarlierNominationInDb = hasEarlierNominationInPending ||
                                           await beatmapEventService.HasEarlierNominationAsync(
                                               qualification.Event.SetId,
                                               nominationCandidate.Event.EventId,
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

            if (DateTimeOffset.TryParse((string?)createdAtRaw, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal, out var createdAt))
                return createdAt;
        }
        catch
        {
            // Ignore malformed payload.
        }

        return null;
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

    private sealed record MapEventInfo(BeatmapsetEvent Event, DateTimeOffset? CreatedAt);

    private sealed record MapMergePlan(
        HashSet<long> NominationEventIdsToSuppress,
        Dictionary<long, BeatmapsetEvent> NominationForQualification);
}
