using System.Globalization;
using System.Text.Json;
using System.Text.Json.Nodes;
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
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);
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
        await EnrichBeatmapsetEventsAsync(parsedEvents, payload.Events, payload.Users, cancellationToken);

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

        await EnrichGroupEventsAsync(parsedEvents, cancellationToken);

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

    private async Task EnrichBeatmapsetEventsAsync(
        IReadOnlyCollection<BeatmapsetEvent> parsedEvents,
        IReadOnlyCollection<OsuBeatmapsetEventsEvent> sourceEvents,
        IReadOnlyCollection<OsuBeatmapsetEventsUser> sourceUsers,
        CancellationToken cancellationToken)
    {
        var sourceById = sourceEvents.ToDictionary(x => x.Id);
        var sourceUserNameById = sourceUsers
            .Where(x => !string.IsNullOrWhiteSpace(x.Username))
            .ToDictionary(x => x.Id, x => x.Username);

        var beatmapsetCache = new Dictionary<long, OsuBeatmapsetInfo?>();
        var profileCache = new Dictionary<long, OsuUserProfileInfo?>();
        var historyCache = new Dictionary<long, IReadOnlyList<OsuBeatmapsetEventsEvent>>();
        var userNameCache = new Dictionary<long, string?>();

        foreach (var parsedEvent in parsedEvents.OrderBy(x => x.EventId))
        {
            sourceById.TryGetValue(parsedEvent.EventId, out var sourceEvent);

            parsedEvent.CreatedAt ??= sourceEvent?.CreatedAt;

            var resolvedMessage = await ResolveMapMessageAsync(parsedEvent, sourceEvent, cancellationToken);
            parsedEvent.Message = NormalizeMapMessage(parsedEvent.EventType, resolvedMessage);

            if (!beatmapsetCache.TryGetValue(parsedEvent.SetId, out var beatmapset))
            {
                beatmapset = await osuApiClient.GetBeatmapsetAsync(parsedEvent.SetId, cancellationToken);
                beatmapsetCache[parsedEvent.SetId] = beatmapset;
            }

            parsedEvent.BeatmapsetTitle = FirstNonEmpty(parsedEvent.BeatmapsetTitle)
                ?? FirstNonEmpty(beatmapset?.Title)
                ?? $"Beatmapset {parsedEvent.SetId}";
            parsedEvent.MapperName = FirstNonEmpty(parsedEvent.MapperName)
                ?? FirstNonEmpty(beatmapset?.Creator)
                ?? "Unknown";

            var resolvedRulesets = await ResolveRulesetsAsync(parsedEvent, cancellationToken);
            parsedEvent.Rulesets = FeedEnumExtensions.SerializeRulesets(resolvedRulesets);

            if (parsedEvent.TriggeredBy is not null)
            {
                var userId = parsedEvent.TriggeredBy.Value;

                if (!profileCache.TryGetValue(userId, out var profile))
                {
                    profile = await osuApiClient.GetUserProfileAsync(userId, cancellationToken);
                    profileCache[userId] = profile;
                }

                parsedEvent.ActorUsername = FirstNonEmpty(parsedEvent.ActorUsername)
                    ?? FirstNonEmpty(profile?.Username)
                    ?? sourceUserNameById.GetValueOrDefault(userId);
                parsedEvent.ActorAvatarUrl = FirstNonEmpty(parsedEvent.ActorAvatarUrl)
                    ?? FirstNonEmpty(profile?.AvatarUrl);
                parsedEvent.ActorBadge = FirstNonEmpty(parsedEvent.ActorBadge)
                    ?? FirstNonEmpty(profile?.Badge);
            }

            if (parsedEvent.EventType != FeedEventType.Ranked)
            {
                parsedEvent.RankedHistoryJson = null;
                continue;
            }

            if (!historyCache.TryGetValue(parsedEvent.SetId, out var completeHistory))
            {
                completeHistory = await osuApiClient.GetCompleteBeatmapsetEventHistoryAsync(parsedEvent.SetId, cancellationToken);
                historyCache[parsedEvent.SetId] = completeHistory;
            }

            var rankedHistory = await BuildRankedHistorySnapshotAsync(
                parsedEvent.EventId,
                completeHistory,
                userNameCache,
                cancellationToken);

            parsedEvent.RankedHistoryJson = rankedHistory.Count == 0
                ? null
                : JsonSerializer.Serialize(rankedHistory, JsonOptions);
        }
    }

    private async Task EnrichGroupEventsAsync(
        IReadOnlyCollection<GroupEvent> parsedEvents,
        CancellationToken cancellationToken)
    {
        var profileCache = new Dictionary<long, OsuUserProfileInfo?>();
        var groupNameCache = new Dictionary<long, string?>();

        foreach (var parsedEvent in parsedEvents.OrderBy(x => x.EventId))
        {
            if (!profileCache.TryGetValue(parsedEvent.UserId, out var profile))
            {
                profile = await osuApiClient.GetUserProfileAsync(parsedEvent.UserId, cancellationToken);
                profileCache[parsedEvent.UserId] = profile;
            }

            parsedEvent.UserName = FirstNonEmpty(parsedEvent.UserName)
                ?? FirstNonEmpty(profile?.Username)
                ?? await osuApiClient.GetUserNameAsync(parsedEvent.UserId, cancellationToken)
                ?? $"User {parsedEvent.UserId}";
            parsedEvent.ActorAvatarUrl = FirstNonEmpty(parsedEvent.ActorAvatarUrl)
                ?? FirstNonEmpty(profile?.AvatarUrl);
            parsedEvent.ActorBadge = FirstNonEmpty(parsedEvent.ActorBadge)
                ?? FirstNonEmpty(profile?.Badge);

            if (!groupNameCache.TryGetValue(parsedEvent.GroupId, out var groupName))
            {
                groupName = await osuApiClient.GetGroupNameAsync(parsedEvent.GroupId, cancellationToken);
                groupNameCache[parsedEvent.GroupId] = groupName;
            }

            parsedEvent.GroupName = FirstNonEmpty(parsedEvent.GroupName)
                ?? FirstNonEmpty(groupName)
                ?? $"Group {parsedEvent.GroupId}";
        }
    }

    private async Task<string?> ResolveMapMessageAsync(
        BeatmapsetEvent parsedEvent,
        OsuBeatmapsetEventsEvent? sourceEvent,
        CancellationToken cancellationToken)
    {
        if (!string.IsNullOrWhiteSpace(parsedEvent.Message))
            return parsedEvent.Message;

        var createdAt = sourceEvent?.CreatedAt ?? parsedEvent.CreatedAt;

        if (parsedEvent.TriggeredBy is not null)
        {
            if (parsedEvent.EventType is FeedEventType.Nomination or FeedEventType.Qualification)
            {
                var praiseOrHypeMessage = await osuApiClient.GetLatestPraiseOrHypeMessageAsync(
                    parsedEvent.SetId,
                    parsedEvent.TriggeredBy.Value,
                    createdAt,
                    cancellationToken);

                if (!string.IsNullOrWhiteSpace(praiseOrHypeMessage))
                    return praiseOrHypeMessage;
            }

            if (parsedEvent.EventType == FeedEventType.Disqualification)
            {
                var discussionMessageByUser = await osuApiClient.GetLatestDiscussionMessageByUserAsync(
                    parsedEvent.SetId,
                    parsedEvent.TriggeredBy.Value,
                    createdAt,
                    cancellationToken);

                if (!string.IsNullOrWhiteSpace(discussionMessageByUser))
                    return discussionMessageByUser;
            }
        }

        if (parsedEvent.PostId is not null || parsedEvent.DiscussionId is not null)
        {
            var discussionMessage = await osuApiClient.GetBeatmapsetDiscussionMessageAsync(
                parsedEvent.SetId,
                parsedEvent.PostId,
                parsedEvent.DiscussionId,
                cancellationToken);

            if (!string.IsNullOrWhiteSpace(discussionMessage))
                return discussionMessage;
        }

        return null;
    }

    private async Task<HashSet<Ruleset>> ResolveRulesetsAsync(
        BeatmapsetEvent parsedEvent,
        CancellationToken cancellationToken)
    {
        var rulesets = FeedEnumExtensions.ExtractRulesets(parsedEvent.RawEvent, parsedEvent.Rulesets);
        if (rulesets.Count > 0)
            return rulesets;

        var apiModes = await osuApiClient.GetBeatmapsetModesFailsafeAsync(
            parsedEvent.SetId,
            parsedEvent.TriggeredBy,
            parsedEvent.CreatedAt,
            cancellationToken);

        foreach (var apiMode in apiModes)
        {
            if (FeedEnumExtensions.TryParseRuleset(apiMode, out var parsedRuleset))
                rulesets.Add(parsedRuleset);
        }

        if (rulesets.Count == 0)
            rulesets.Add(Ruleset.Osu);

        return rulesets;
    }

    private async Task<IReadOnlyList<RankedHistorySnapshot>> BuildRankedHistorySnapshotAsync(
        long eventId,
        IReadOnlyList<OsuBeatmapsetEventsEvent> completeHistory,
        Dictionary<long, string?> userNameCache,
        CancellationToken cancellationToken)
    {
        var relevantHistory = completeHistory
            .Where(x => x.Id <= eventId)
            .Select(x => new
            {
                Event = x,
                Type = MapHistoryEventType(x.Type),
            })
            .Where(x => x.Type is not null)
            .Select(x => new RankedHistoryEvent(x.Event, x.Type!.Value))
            .OrderBy(x => x.Event.Id)
            .ToList();

        if (relevantHistory.Count == 0)
            return [];

        var historyWithActors = relevantHistory
            .Select((x, index) => new RankedHistoryEntry(
                x.Event,
                x.Type,
                ResolveHistoryUserId(relevantHistory, index)))
            .ToList();
        historyWithActors = CoalesceRankedHistory(historyWithActors);

        var lastQualification = historyWithActors.LastOrDefault(x => x.Type == FeedEventType.Qualification);
        var trimmedHistory = historyWithActors
            .OrderByDescending(x => x.Event.Id)
            .Take(8)
            .OrderBy(x => x.Event.Id)
            .ToList();

        if (lastQualification is not null &&
            trimmedHistory.All(x => x.Event.Id != lastQualification.Event.Id))
        {
            trimmedHistory.RemoveAt(0);
            trimmedHistory.Add(lastQualification);
            trimmedHistory = trimmedHistory
                .OrderBy(x => x.Event.Id)
                .ToList();
        }

        var actions = new List<RankedHistorySnapshot>();
        foreach (var historyEvent in trimmedHistory)
        {
            string? userName = null;
            if (historyEvent.UserId is not null)
            {
                var userId = historyEvent.UserId.Value;
                if (!userNameCache.TryGetValue(userId, out userName))
                {
                    userName = await osuApiClient.GetUserNameAsync(userId, cancellationToken);
                    userNameCache[userId] = userName;
                }
            }

            actions.Add(new RankedHistorySnapshot(
                historyEvent.Type,
                historyEvent.UserId,
                string.IsNullOrWhiteSpace(userName) ? null : userName));
        }

        return actions;
    }

    private static List<RankedHistoryEntry> CoalesceRankedHistory(IReadOnlyList<RankedHistoryEntry> entries)
    {
        var ordered = entries
            .OrderBy(x => x.Event.Id)
            .ToList();

        var coalesced = new List<RankedHistoryEntry>();
        foreach (var entry in ordered)
        {
            if (coalesced.Count == 0)
            {
                coalesced.Add(entry);
                continue;
            }

            var previous = coalesced[^1];

            if (entry.Type == FeedEventType.Qualification &&
                previous.Type == FeedEventType.Nomination &&
                IsLikelyLinkedNominationAndQualification(previous, entry))
            {
                coalesced[^1] = entry;
                continue;
            }

            if (AreLikelyDuplicateHistoryEntries(previous, entry))
                continue;

            coalesced.Add(entry);
        }

        return coalesced;
    }

    private static bool IsLikelyLinkedNominationAndQualification(
        RankedHistoryEntry nomination,
        RankedHistoryEntry qualification)
    {
        var nominationUserId = nomination.UserId;
        var qualificationUserId = qualification.UserId;

        if (nominationUserId is not null &&
            qualificationUserId is not null &&
            nominationUserId != qualificationUserId)
        {
            return false;
        }

        return IsCloseInTime(nomination.Event.CreatedAt, qualification.Event.CreatedAt, TimeSpan.FromMinutes(2));
    }

    private static bool AreLikelyDuplicateHistoryEntries(
        RankedHistoryEntry previous,
        RankedHistoryEntry current)
    {
        if (previous.Type != current.Type)
            return false;

        if (current.Type == FeedEventType.Nomination)
            return false;

        if (previous.UserId is not null &&
            current.UserId is not null &&
            previous.UserId != current.UserId)
        {
            return false;
        }

        return IsCloseInTime(previous.Event.CreatedAt, current.Event.CreatedAt, TimeSpan.FromMinutes(2));
    }

    private static bool IsCloseInTime(
        DateTimeOffset? earlier,
        DateTimeOffset? later,
        TimeSpan maxGap)
    {
        if (earlier is null || later is null)
            return false;

        var delta = later.Value - earlier.Value;
        if (delta < TimeSpan.Zero)
            return false;

        return delta <= maxGap;
    }

    private static FeedEventType? MapHistoryEventType(string rawType)
    {
        return rawType.Trim().ToLowerInvariant() switch
        {
            "nominate" => FeedEventType.Nomination,
            "nomination_reset" => FeedEventType.NominationReset,
            "qualify" => FeedEventType.Qualification,
            "disqualify" => FeedEventType.Disqualification,
            _ => null,
        };
    }

    private static long? ResolveHistoryUserId(
        IReadOnlyList<RankedHistoryEvent> relevantHistory,
        int index)
    {
        var historyEvent = relevantHistory[index];
        var mappedType = historyEvent.Type;
        var sourceEvent = historyEvent.Event;

        if (sourceEvent.UserId is not null)
            return sourceEvent.UserId.Value;

        if (mappedType != FeedEventType.Qualification)
            return null;

        for (var i = index - 1; i >= 0; i--)
        {
            var (candidateEvent, candidateType) = relevantHistory[i];

            if (candidateType != FeedEventType.Nomination || candidateEvent.UserId is null)
                continue;

            if (sourceEvent.CreatedAt is not null && candidateEvent.CreatedAt is not null)
            {
                var delta = sourceEvent.CreatedAt.Value - candidateEvent.CreatedAt.Value;
                if (delta < TimeSpan.Zero)
                    continue;

                if (delta > TimeSpan.FromMinutes(2))
                    continue;
            }

            return candidateEvent.UserId.Value;
        }

        return null;
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

            if (string.IsNullOrWhiteSpace(qualificationEvent.ActorUsername))
                qualificationEvent.ActorUsername = nominationCandidate.Event.ActorUsername;

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
            MapperName = TryGetMapperName(payload.RawJson),
            BeatmapsetTitle = TryGetBeatmapsetTitle(payload.RawJson),
            ActorUsername = TryGetActorUsername(payload.RawJson),
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
            ActorAvatarUrl = payload.TryGetNestedString("user", "avatar_url"),
            ActorBadge = payload.TryGetNestedString("user", "title"),
            CreatedAt = TryParseCreatedAt(payload.TryGetString("created_at")),
            GroupId = groupId.Value,
            GroupName = payload.TryGetString("group_name")
                ?? payload.TryGetNestedString("group", "short_name")
                ?? payload.TryGetNestedString("group", "name"),
            Playmodes = TryExtractGroupPlaymodes(payload),
            EventType = eventType.Value,
            RawEvent = payload.ToJsonString(JsonOptions),
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

    private static string? TryGetMapperName(string rawEvent)
    {
        try
        {
            var root = JsonNode.Parse(rawEvent) as JsonObject;
            return FirstNonEmpty(root?.TryGetNestedString("beatmapset", "creator"));
        }
        catch
        {
            return null;
        }
    }

    private static string? TryGetBeatmapsetTitle(string rawEvent)
    {
        try
        {
            var root = JsonNode.Parse(rawEvent) as JsonObject;
            var artist = root?.TryGetNestedString("beatmapset", "artist");
            var title = root?.TryGetNestedString("beatmapset", "title");
            if (!string.IsNullOrWhiteSpace(artist) && !string.IsNullOrWhiteSpace(title))
                return $"{artist} - {title}";

            return FirstNonEmpty(title);
        }
        catch
        {
            return null;
        }
    }

    private static string? TryGetActorUsername(string rawEvent)
    {
        try
        {
            var root = JsonNode.Parse(rawEvent) as JsonObject;
            return FirstNonEmpty(root?.TryGetNestedString("user", "username"));
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

    private static string? NormalizeMapMessage(FeedEventType eventType, string? message)
    {
        if (eventType is not (FeedEventType.Nomination or FeedEventType.Qualification or FeedEventType.Disqualification))
            return null;

        if (string.IsNullOrWhiteSpace(message))
            return null;

        var normalized = string.Join(' ', message
            .Split(['\r', '\n'], StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries));

        if (string.IsNullOrWhiteSpace(normalized))
            return null;

        return Trim(normalized, 220);
    }

    private static string Trim(string value, int maxLength)
    {
        if (value.Length <= maxLength)
            return value;

        return value[..Math.Max(0, maxLength - 3)] + "...";
    }

    private static string? FirstNonEmpty(string? value)
    {
        return string.IsNullOrWhiteSpace(value) ? null : value;
    }

    private sealed record RankedHistorySnapshot(
        FeedEventType Action,
        long? UserId,
        string? Username);

    private sealed record RankedHistoryEvent(OsuBeatmapsetEventsEvent Event, FeedEventType Type);

    private sealed record RankedHistoryEntry(OsuBeatmapsetEventsEvent Event, FeedEventType Type, long? UserId);
}
