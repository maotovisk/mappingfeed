using System.Globalization;
using System.Text.Json;
using System.Text.Json.Nodes;
using MappingFeed.Common.Config;
using MappingFeed.Common.Data.Entities;
using MappingFeed.Common.Data.Enums;
using MappingFeed.Common.Data.TransitionalRecords;
using MappingFeed.Common.Json;
using MappingFeed.Common.Services.Beatmap;
using MappingFeed.Common.Services.Osu;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace MappingFeed.Scraper.Fetchers.Events;

public sealed class BeatmapEventsFetcher(
    IBeatmapEventService beatmapEventService,
    IOsuApiService osuApiService,
    IOptions<FeedOptions> options,
    ILogger<BeatmapEventsFetcher> logger)
{
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);
    private readonly FeedOptions _options = options.Value;

    public async Task FetchAsync(CancellationToken cancellationToken)
    {
        logger.LogInformation("Fetching beatmapset events.");
        var payload = await osuApiService.GetBeatmapEventsAsync(_options.EventsBatchSize, cancellationToken);

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

        var newEvents = await beatmapEventService.SaveNewEventsAsync(parsedEvents, cancellationToken);
        if (newEvents.Count == 0)
            return;

        logger.LogInformation("Fetched {Count} beatmapset events.", newEvents.Count);
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
                beatmapset = await osuApiService.GetBeatmapAsync(parsedEvent.SetId, cancellationToken);
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
                    profile = await osuApiService.GetUserAsync(userId, cancellationToken);
                    profileCache[userId] = profile;
                }

                parsedEvent.ActorUsername = FirstNonEmpty(parsedEvent.ActorUsername)
                    ?? FirstNonEmpty(profile?.Username)
                    ?? sourceUserNameById.GetValueOrDefault(userId);
                parsedEvent.ActorAvatarUrl = FirstNonEmpty(parsedEvent.ActorAvatarUrl)
                    ?? FirstNonEmpty(profile?.AvatarUrl);
                parsedEvent.ActorBadge = FirstNonEmpty(parsedEvent.ActorBadge)
                    ?? FirstNonEmpty(profile?.Badge);
                parsedEvent.ActorColor = FirstNonEmpty(parsedEvent.ActorColor)
                    ?? FirstNonEmpty(profile?.Color);
            }

            if (parsedEvent.EventType != FeedEventType.Ranked)
            {
                parsedEvent.RankedHistoryJson = null;
                continue;
            }

            if (!historyCache.TryGetValue(parsedEvent.SetId, out var completeHistory))
            {
                completeHistory = await osuApiService.GetCompleteBeatmapsetEventHistoryAsync(parsedEvent.SetId, cancellationToken);
                historyCache[parsedEvent.SetId] = completeHistory;
            }

            var rankedHistory = await BuildRankedHistorySnapshotAsync(
                parsedEvent.EventId,
                completeHistory,
                profileCache,
                userNameCache,
                cancellationToken);

            parsedEvent.RankedHistoryJson = rankedHistory.Count == 0
                ? null
                : JsonSerializer.Serialize(rankedHistory, JsonOptions);
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
                var praiseOrHypeMessage = await osuApiService.GetLatestPraiseOrHypeMessageAsync(
                    parsedEvent.SetId,
                    parsedEvent.TriggeredBy.Value,
                    createdAt,
                    cancellationToken);

                if (!string.IsNullOrWhiteSpace(praiseOrHypeMessage))
                    return praiseOrHypeMessage;
            }

            if (parsedEvent.EventType is FeedEventType.Disqualification or FeedEventType.NominationReset)
            {
                var discussionMessageByUser = await osuApiService.GetLatestDiscussionMessageByUserAsync(
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
            var discussionMessage = await osuApiService.GetBeatmapsetDiscussionMessageAsync(
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

        var apiModes = await osuApiService.GetBeatmapsetModesFailsafeAsync(
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
        Dictionary<long, OsuUserProfileInfo?> userProfileCache,
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
            string? userColor = null;
            if (historyEvent.UserId is not null)
            {
                var userId = historyEvent.UserId.Value;
                if (!userProfileCache.TryGetValue(userId, out var profile))
                {
                    profile = await osuApiService.GetUserAsync(userId, cancellationToken);
                    userProfileCache[userId] = profile;
                }

                userColor = profile?.Color;

                if (!userNameCache.TryGetValue(userId, out userName))
                {
                    userName = FirstNonEmpty(profile?.Username)
                        ?? await osuApiService.GetUserNameAsync(userId, cancellationToken);
                    userNameCache[userId] = userName;
                }
            }

            actions.Add(new RankedHistorySnapshot(
                historyEvent.Type,
                historyEvent.UserId,
                string.IsNullOrWhiteSpace(userName) ? null : userName,
                FirstNonEmpty(userColor)));
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
            ActorColor = TryGetActorColor(payload.RawJson),
            Rulesets = TrySerializeRulesets(payload.RawJson),
            RawEvent = payload.RawJson,
            EventId = payload.Id,
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

    private static string? TryGetActorColor(string rawEvent)
    {
        try
        {
            var root = JsonNode.Parse(rawEvent) as JsonObject;
            return root is null ? null : TryExtractActorColor(root);
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

    private static string? TryExtractActorColor(JsonObject payload)
    {
        var rawColor = payload.TryGetString("user_color", "user_colour")
            ?? payload.TryGetNestedString("user", "color")
            ?? payload.TryGetNestedString("user", "colour");

        var normalized = NormalizeColor(rawColor);
        if (!string.IsNullOrWhiteSpace(normalized))
            return normalized;

        var user = payload["user"] as JsonObject;
        if (user?["groups"] is not JsonArray groups)
            return null;

        foreach (var group in groups.OfType<JsonObject>())
        {
            var groupColor = NormalizeColor(group.TryGetString("color", "colour"));
            if (!string.IsNullOrWhiteSpace(groupColor))
                return groupColor;
        }

        return null;
    }

    private static string? NormalizeColor(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
            return null;

        var trimmed = value.Trim();
        if (trimmed.StartsWith('#'))
            trimmed = trimmed[1..];

        if (trimmed.Length == 3 && trimmed.All(Uri.IsHexDigit))
            trimmed = string.Concat(trimmed.Select(x => $"{x}{x}"));

        if (trimmed.Length != 6 || !trimmed.All(Uri.IsHexDigit))
            return null;

        return $"#{trimmed.ToUpperInvariant()}";
    }

    private static string? NormalizeMapMessage(FeedEventType eventType, string? message)
    {
        if (eventType is not (FeedEventType.Nomination or FeedEventType.Qualification or FeedEventType.Disqualification or FeedEventType.NominationReset))
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
        string? Username,
        string? UserColor);

    private sealed record RankedHistoryEvent(OsuBeatmapsetEventsEvent Event, FeedEventType Type);

    private sealed record RankedHistoryEntry(OsuBeatmapsetEventsEvent Event, FeedEventType Type, long? UserId);
}
