using System.Text.Json.Nodes;
using System.Globalization;
using MappingFeed.Config;
using MappingFeed.Data;
using MappingFeed.Data.Entities;
using MappingFeed.Osu;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;

namespace MappingFeed.Feed;

public sealed class FeedFetchingWorker(
    IDbContextFactory<MappingFeedDbContext> dbContextFactory,
    OsuApiClient osuApiClient,
    IOptions<FeedOptions> options,
    ILogger<FeedFetchingWorker> logger) : BackgroundService
{
    private readonly FeedOptions _options = options.Value;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await FetchBeatmapsetEventsAsync(stoppingToken);
                await FetchGroupEventsAsync(stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception exception)
            {
                logger.LogError(exception, "Failed while fetching osu! feed events.");
            }

            await Task.Delay(TimeSpan.FromSeconds(_options.PollIntervalSeconds), stoppingToken);
        }
    }

    private async Task FetchBeatmapsetEventsAsync(CancellationToken cancellationToken)
    {
        logger.LogInformation("Fetching beatmapset events.");
        var payload = await osuApiClient.GetBeatmapsetEventsAsync(_options.EventsBatchSize, cancellationToken);

        var parsedEvents = payload.Events
            .Select(ParseBeatmapsetEvent)
            .Where(x => x is not null)
            .Select(x => x!)
            .OrderBy(x => x.EventId)
            .ToList();

        if (parsedEvents.Count == 0)
            return;

        CoalesceQualificationWithNomination(parsedEvents, payload.Events);
        await EnrichMapMessagesAsync(parsedEvents, payload.Events, cancellationToken);

        await using var db = await dbContextFactory.CreateDbContextAsync(cancellationToken);

        var incomingEventIds = parsedEvents.Select(x => x.EventId).ToHashSet();
        var existingIdSet = await db.BeatmapsetEvents
            .Where(x => incomingEventIds.Contains(x.EventId))
            .Select(x => x.EventId)
            .ToHashSetAsync(cancellationToken);

        var newEvents = parsedEvents
            .Where(x => !existingIdSet.Contains(x.EventId))
            .ToList();

        if (newEvents.Count == 0)
            return;

        db.BeatmapsetEvents.AddRange(newEvents);
        await db.SaveChangesAsync(cancellationToken);
        logger.LogInformation("Fetched {Count} beatmapset events.", newEvents.Count);
    }

    private async Task FetchGroupEventsAsync(CancellationToken cancellationToken)
    {
        logger.LogInformation("Fetching group events.");
        var payloads = await osuApiClient.GetGroupHistoryEventsAsync(_options.EventsBatchSize, cancellationToken);

        var parsedEvents = payloads
            .Select(ParseGroupEvent)
            .Where(x => x is not null)
            .Select(x => x!)
            .OrderBy(x => x.EventId)
            .ToList();

        if (parsedEvents.Count == 0)
            return;

        await using var db = await dbContextFactory.CreateDbContextAsync(cancellationToken);

        var incomingEventIds = parsedEvents.Select(x => x.EventId).ToHashSet();
        var existingIdSet = await db.GroupEvents
            .Where(x => incomingEventIds.Contains(x.EventId))
            .Select(x => x.EventId)
            .ToHashSetAsync(cancellationToken);

        var newEvents = parsedEvents
            .Where(x => !existingIdSet.Contains(x.EventId))
            .ToList();

        if (newEvents.Count == 0)
            return;

        db.GroupEvents.AddRange(newEvents);
        await db.SaveChangesAsync(cancellationToken);

        logger.LogInformation("Fetched {Count} group events.", newEvents.Count);
    }

    private async Task EnrichMapMessagesAsync(
        IReadOnlyCollection<BeatmapsetEvent> parsedEvents,
        IReadOnlyCollection<OsuBeatmapsetEventsEvent> sourceEvents,
        CancellationToken cancellationToken)
    {
        var sourceById = sourceEvents.ToDictionary(x => x.Id);

        foreach (var parsedEvent in parsedEvents)
        {
            if (parsedEvent.EventType is not (
                    FeedEventType.Nomination
                    or FeedEventType.Qualification
                    or FeedEventType.Disqualification) ||
                parsedEvent.TriggeredBy is null ||
                !string.IsNullOrWhiteSpace(parsedEvent.Message))
            {
                continue;
            }

            if (!sourceById.TryGetValue(parsedEvent.EventId, out var sourceEvent))
                continue;

            if (parsedEvent.EventType is FeedEventType.Nomination or FeedEventType.Qualification)
            {
                var praiseOrHypeMessage = await osuApiClient.GetLatestPraiseOrHypeMessageAsync(
                    parsedEvent.SetId,
                    parsedEvent.TriggeredBy.Value,
                    sourceEvent.CreatedAt,
                    cancellationToken);

                if (!string.IsNullOrWhiteSpace(praiseOrHypeMessage))
                    parsedEvent.Message = praiseOrHypeMessage;
                continue;
            }

            var discussionMessage = await osuApiClient.GetLatestDiscussionMessageByUserAsync(
                parsedEvent.SetId,
                parsedEvent.TriggeredBy.Value,
                sourceEvent.CreatedAt,
                cancellationToken);

            if (!string.IsNullOrWhiteSpace(discussionMessage))
                parsedEvent.Message = discussionMessage;
        }
    }

    private static void CoalesceQualificationWithNomination(
        List<BeatmapsetEvent> parsedEvents,
        IReadOnlyCollection<OsuBeatmapsetEventsEvent> sourceEvents)
    {
        var sourceById = sourceEvents.ToDictionary(x => x.Id);
        var nominationIdsToRemove = new HashSet<long>();

        foreach (var qualificationEvent in parsedEvents
                     .Where(x => x.EventType == FeedEventType.Qualification)
                     .OrderBy(x => x.EventId))
        {
            if (!sourceById.TryGetValue(qualificationEvent.EventId, out var qualificationSource))
                continue;

            var nominationCandidate = parsedEvents
                .Where(x =>
                    x.EventType == FeedEventType.Nomination &&
                    x.SetId == qualificationEvent.SetId &&
                    x.EventId < qualificationEvent.EventId &&
                    !nominationIdsToRemove.Contains(x.EventId))
                .Select(x => new
                {
                    Event = x,
                    Source = sourceById.TryGetValue(x.EventId, out var nominationSource) ? nominationSource : null,
                })
                .Where(x => x.Source is not null)
                .Where(x => IsQualificationFromNomination(x.Source!.CreatedAt, qualificationSource.CreatedAt))
                .OrderByDescending(x => x.Event.EventId)
                .FirstOrDefault();

            if (nominationCandidate is null)
                continue;

            if (qualificationEvent.TriggeredBy is null)
                qualificationEvent.TriggeredBy = nominationCandidate.Event.TriggeredBy;

            nominationIdsToRemove.Add(nominationCandidate.Event.EventId);
        }

        if (nominationIdsToRemove.Count > 0)
            parsedEvents.RemoveAll(x => nominationIdsToRemove.Contains(x.EventId));
    }

    private static bool IsQualificationFromNomination(
        DateTimeOffset? nominationCreatedAt,
        DateTimeOffset? qualificationCreatedAt)
    {
        if (nominationCreatedAt is null || qualificationCreatedAt is null)
            return false;

        var delta = qualificationCreatedAt.Value - nominationCreatedAt.Value;
        return delta >= TimeSpan.Zero && delta <= TimeSpan.FromSeconds(45);
    }

    private static BeatmapsetEvent? ParseBeatmapsetEvent(OsuBeatmapsetEventsEvent payload)
    {
        var eventType = MapBeatmapsetEventType(payload.Type);
        if (eventType is null)
            return null;

        return new BeatmapsetEvent
        {
            SetId = payload.BeatmapsetId,
            TriggeredBy = payload.UserId,
            CreatedAt = payload.CreatedAt,
            EventType = eventType.Value,
            Message = payload.Message,
            DiscussionId = payload.DiscussionId,
            PostId = payload.DiscussionPostId,
            MapperUserId = TryGetMapperUserId(payload.RawJson),
            Rulesets = TrySerializeRulesets(payload.RawJson),
            RawEvent = payload.RawJson,
            EventId = payload.Id,
        };
    }

    private static GroupEvent? ParseGroupEvent(JsonObject payload)
    {
        var eventId = payload.TryGetInt64("id");
        var userId = payload.TryGetInt64("user_id") ?? payload.TryGetNestedInt64("user", "id");
        var groupId = payload.TryGetInt64("group_id");
        var rawType = payload.TryGetString("type") ?? string.Empty;
        var eventType = MapGroupEventType(rawType);

        if (eventId is null || userId is null || groupId is null || eventType is null)
            return null;

        return new GroupEvent
        {
            EventId = eventId.Value,
            UserId = userId.Value,
            UserName = payload.TryGetString("user_name") ?? payload.TryGetNestedString("user", "username"),
            CreatedAt = TryParseCreatedAt(payload.TryGetString("created_at")),
            GroupId = groupId.Value,
            GroupName = payload.TryGetString("group_name")
                ?? payload.TryGetNestedString("group", "short_name")
                ?? payload.TryGetNestedString("group", "name"),
            Playmodes = TryExtractGroupPlaymodes(payload),
            EventType = eventType.Value,
            RawEvent = payload.ToJsonString(),
        };
    }
    
    private static FeedEventType? MapBeatmapsetEventType(string rawType)
    {
        return rawType.Trim().ToLowerInvariant() switch
        {
            "nominate" => FeedEventType.Nomination,
            "nomination_reset" => FeedEventType.NominationReset,
            "qualify" => FeedEventType.Qualification,
            "disqualify" => FeedEventType.Disqualification,
            "rank" => FeedEventType.Ranked,
            "unrank" => FeedEventType.Unranked,
            _ => null,
        };
    }

    private static FeedEventType? MapGroupEventType(string rawType)
    {
        return rawType.Trim().ToLowerInvariant() switch
        {
            "user_add" or "user_add_playmodes" => FeedEventType.GroupAdd,
            "user_remove" or "user_remove_playmodes" => FeedEventType.GroupRemove,
            _ => null,
        };
    }

    private static long? TryGetMapperUserId(string rawEvent)
    {
        try
        {
            var root = JsonNode.Parse(rawEvent) as JsonObject;
            return root?.TryGetNestedInt64("beatmapset", "user_id");
        }
        catch
        {
            return null;
        }
    }

    private static string? TrySerializeRulesets(string rawEvent)
    {
        return FeedEnumExtensions.SerializeRulesets(FeedEnumExtensions.ExtractRulesets(rawEvent));
    }

    private static DateTimeOffset? TryParseCreatedAt(string? rawCreatedAt)
    {
        if (string.IsNullOrWhiteSpace(rawCreatedAt))
            return null;

        return DateTimeOffset.TryParse(rawCreatedAt, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal, out var createdAt)
            ? createdAt
            : null;
    }

    private static string? TryExtractGroupPlaymodes(JsonObject payload)
    {
        if (payload["playmodes"] is not JsonArray playmodes)
            return null;

        var normalized = new List<string>();
        foreach (var playmode in playmodes)
        {
            if (playmode is null)
                continue;

            var value = playmode.ToString();
            if (FeedEnumExtensions.TryParseRuleset(value, out var ruleset))
                normalized.Add(ruleset.ToCommandValue());
            else if (!string.IsNullOrWhiteSpace(value))
                normalized.Add(value.ToLowerInvariant());
        }

        return normalized.Count == 0
            ? null
            : string.Join(", ", normalized.Distinct());
    }
}
