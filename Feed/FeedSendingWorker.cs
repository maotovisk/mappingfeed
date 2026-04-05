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
    OsuApiClient osuApiClient,
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
                subscription.LastEventId = pendingEvent.EventId;
                await db.SaveChangesAsync(cancellationToken);
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
            if (pendingEvent.EventType == FeedEventType.Qualification &&
                hasMergedNomination &&
                TryExtractRulesets(rawEventForRuleset).Count == 0)
            {
                rawEventForRuleset = mergedNomination!.RawEvent;
            }

            if (!await ShouldDispatchToRulesetsAsync(
                    allowedRulesets,
                    pendingEvent,
                    rawEventForRuleset,
                    cancellationToken))
            {
                subscription.LastEventId = pendingEvent.EventId;
                await db.SaveChangesAsync(cancellationToken);
                continue;
            }

            if (!ShouldDispatchToEventTypes(allowedEventTypes, pendingEvent.EventType))
            {
                subscription.LastEventId = pendingEvent.EventId;
                await db.SaveChangesAsync(cancellationToken);
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

                subscription.LastEventId = pendingEvent.EventId;
                await db.SaveChangesAsync(cancellationToken);
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

    private async Task<bool> ShouldDispatchToRulesetsAsync(
        HashSet<Ruleset>? allowedRulesets,
        BeatmapsetEvent pendingEvent,
        string rawEvent,
        CancellationToken cancellationToken)
    {
        if (allowedRulesets is null || allowedRulesets.Count == 0)
            return true;

        var eventRulesets = TryExtractRulesets(rawEvent);
        if (eventRulesets.Count > 0)
            return eventRulesets.Overlaps(allowedRulesets);

        if (!ShouldUseRulesetFailsafe(pendingEvent.EventType))
            return false;

        var eventCreatedAt = TryGetCreatedAt(pendingEvent.RawEvent);
        var fallbackModes = await osuApiClient.GetBeatmapsetModesFailsafeAsync(
            pendingEvent.SetId,
            pendingEvent.TriggeredBy,
            eventCreatedAt,
            cancellationToken);

        foreach (var fallbackMode in fallbackModes)
        {
            if (FeedEnumExtensions.TryParseRuleset(fallbackMode, out var fallbackRuleset) &&
                allowedRulesets.Contains(fallbackRuleset))
            {
                return true;
            }
        }

        return false;
    }

    private static bool ShouldUseRulesetFailsafe(FeedEventType eventType)
    {
        return eventType is FeedEventType.Nomination
            or FeedEventType.Qualification
            or FeedEventType.Disqualification;
    }

    private static bool ShouldDispatchToEventTypes(HashSet<FeedEventType>? allowedEventTypes, FeedEventType eventType)
    {
        if (allowedEventTypes is null || allowedEventTypes.Count == 0)
            return true;

        return allowedEventTypes.Contains(eventType);
    }

    private async Task<MapMergePlan> BuildMapMergePlanAsync(
        MappingFeedDbContext db,
        IReadOnlyCollection<BeatmapsetEvent> pendingEvents,
        CancellationToken cancellationToken)
    {
        var eventInfos = pendingEvents
            .Select(x => new MapEventInfo(x, TryGetCreatedAt(x.RawEvent)))
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

    private static HashSet<Ruleset> TryExtractRulesets(string rawEvent)
    {
        var rulesets = new HashSet<Ruleset>();

        try
        {
            var root = JsonNode.Parse(rawEvent) as JsonObject;
            AddRulesetsFromCommentModes(root, rulesets);

            var mode = root?.TryGetNestedString("beatmap", "mode")
                ?? root?.TryGetString("mode");

            if (FeedEnumExtensions.TryParseRuleset(mode, out var ruleset))
                rulesets.Add(ruleset);

            var modeInt = root?.TryGetNestedInt64("beatmap", "mode_int")
                ?? root?.TryGetInt64("mode_int", "ruleset_id");

            switch (modeInt)
            {
                case 0:
                    rulesets.Add(Ruleset.Osu);
                    break;
                case 1:
                    rulesets.Add(Ruleset.Taiko);
                    break;
                case 2:
                    rulesets.Add(Ruleset.Catch);
                    break;
                case 3:
                    rulesets.Add(Ruleset.Mania);
                    break;
            }
        }
        catch
        {
            // Ignore malformed payload.
        }

        return rulesets;
    }

    private static void AddRulesetsFromCommentModes(JsonObject? root, ISet<Ruleset> destination)
    {
        if (root?["comment"]?["modes"] is not JsonArray modes)
            return;

        foreach (var mode in modes)
        {
            if (mode is null)
                continue;

            var modeValue = mode.ToString();
            if (FeedEnumExtensions.TryParseRuleset(modeValue, out var parsed))
                destination.Add(parsed);
        }
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
                subscription.LastEventId = pendingEvent.EventId;
                await db.SaveChangesAsync(cancellationToken);
                continue;
            }

            if (allowedGroupIds is not null && !allowedGroupIds.Contains(pendingEvent.GroupId))
            {
                subscription.LastEventId = pendingEvent.EventId;
                await db.SaveChangesAsync(cancellationToken);
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

                subscription.LastEventId = pendingEvent.EventId;
                await db.SaveChangesAsync(cancellationToken);
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

    private sealed record MapEventInfo(BeatmapsetEvent Event, DateTimeOffset? CreatedAt);

    private sealed record MapMergePlan(
        HashSet<long> NominationEventIdsToSuppress,
        Dictionary<long, BeatmapsetEvent> NominationForQualification);
}
