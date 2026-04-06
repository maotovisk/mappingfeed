using System.Globalization;
using System.Text.Json.Nodes;
using MappingFeed.Config;
using MappingFeed.Data;
using MappingFeed.Data.Entities;
using MappingFeed.Osu;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Options;

namespace MappingFeed.Feed;

public sealed class FeedEventViewFactory(
    OsuApiClient osuApiClient,
    IDbContextFactory<MappingFeedDbContext> dbContextFactory,
    IMemoryCache memoryCache,
    IOptions<FeedOptions> feedOptions)
{
    private readonly TimeSpan _cacheDuration = TimeSpan.FromMinutes(Math.Clamp(feedOptions.Value.ApiCacheMinutes, 5, 20));

    public Task<FeedEventViewEntry> CreateBeatmapsetEventEntryAsync(
        BeatmapsetEvent beatmapsetEvent,
        CancellationToken cancellationToken)
    {
        var cacheKey = BuildMapCacheKey(beatmapsetEvent);

        return memoryCache.GetOrCreateAsync(cacheKey, async entry =>
        {
            entry.AbsoluteExpirationRelativeToNow = _cacheDuration;
            return await BuildBeatmapsetEventEntryAsync(beatmapsetEvent, cancellationToken);
        })!;
    }

    public Task<FeedEventViewEntry> CreateGroupEventEntryAsync(
        GroupEvent groupEvent,
        CancellationToken cancellationToken)
    {
        var cacheKey = BuildGroupCacheKey(groupEvent);

        return memoryCache.GetOrCreateAsync(cacheKey, async entry =>
        {
            entry.AbsoluteExpirationRelativeToNow = _cacheDuration;
            return await BuildGroupEventEntryAsync(groupEvent, cancellationToken);
        })!;
    }

    private async Task<FeedEventViewEntry> BuildBeatmapsetEventEntryAsync(
        BeatmapsetEvent beatmapsetEvent,
        CancellationToken cancellationToken)
    {
        var beatmapset = await osuApiClient.GetBeatmapsetAsync(beatmapsetEvent.SetId, cancellationToken);
        var actorProfile = beatmapsetEvent.TriggeredBy is null
            ? null
            : await osuApiClient.GetUserProfileAsync(beatmapsetEvent.TriggeredBy.Value, cancellationToken);
        var createdAt = beatmapsetEvent.CreatedAt ?? TryGetCreatedAt(beatmapsetEvent.RawEvent);
        var discussionPostId = beatmapsetEvent.PostId ?? TryGetDiscussionPostId(beatmapsetEvent.RawEvent);
        var discussionId = beatmapsetEvent.DiscussionId ?? TryGetDiscussionId(beatmapsetEvent.RawEvent);
        var resolvedMessage = beatmapsetEvent.Message;

        if (string.IsNullOrWhiteSpace(resolvedMessage))
        {
            resolvedMessage = await osuApiClient.GetBeatmapsetDiscussionMessageAsync(
                beatmapsetEvent.SetId,
                discussionPostId,
                discussionId,
                cancellationToken);
        }

        if (string.IsNullOrWhiteSpace(resolvedMessage) &&
            beatmapsetEvent.EventType == FeedEventType.Disqualification &&
            beatmapsetEvent.TriggeredBy is not null)
        {
            resolvedMessage = await osuApiClient.GetLatestDiscussionMessageByUserAsync(
                beatmapsetEvent.SetId,
                beatmapsetEvent.TriggeredBy.Value,
                createdAt,
                cancellationToken);
        }

        var modes = await ResolveModesAsync(beatmapsetEvent, createdAt, cancellationToken);
        var mapTitle = beatmapset?.Title ?? $"Beatmapset {beatmapsetEvent.SetId}";
        var mapper = beatmapset?.Creator ?? "Unknown";
        var mapperId = beatmapsetEvent.MapperUserId ?? TryGetMapperUserId(beatmapsetEvent.RawEvent);
        var beatmapsetUrl = $"https://osu.ppy.sh/beatmapsets/{beatmapsetEvent.SetId}";
        var rankedHistory = await BuildRankedHistoryAsync(beatmapsetEvent, cancellationToken);
        var message = NormalizeMapMessage(beatmapsetEvent.EventType, resolvedMessage);

        return new FeedEventViewEntry(
            beatmapsetEvent.EventId,
            FeedType.Map,
            beatmapsetEvent.EventType,
            createdAt,
            beatmapsetUrl,
            new FeedEventActor(
                beatmapsetEvent.TriggeredBy,
                actorProfile?.Username,
                actorProfile?.AvatarUrl,
                actorProfile?.Badge),
            new FeedMapEventViewData(
                beatmapsetEvent.SetId,
                beatmapsetUrl,
                mapTitle,
                mapper,
                mapperId,
                modes,
                message,
                rankedHistory),
            null);
    }

    private async Task<FeedEventViewEntry> BuildGroupEventEntryAsync(
        GroupEvent groupEvent,
        CancellationToken cancellationToken)
    {
        var userProfile = await osuApiClient.GetUserProfileAsync(groupEvent.UserId, cancellationToken);
        var userName = groupEvent.UserName
            ?? userProfile?.Username
            ?? await osuApiClient.GetUserNameAsync(groupEvent.UserId, cancellationToken)
            ?? $"User {groupEvent.UserId}";
        var groupName = groupEvent.GroupName
            ?? TryGetGroupName(groupEvent.RawEvent)
            ?? await osuApiClient.GetGroupNameAsync(groupEvent.GroupId, cancellationToken)
            ?? $"Group {groupEvent.GroupId}";
        var playmodes = ParsePlaymodes(groupEvent.Playmodes);
        if (playmodes.Count == 0)
            playmodes = TryGetGroupPlaymodes(groupEvent.RawEvent);
        var createdAt = groupEvent.CreatedAt ?? TryGetCreatedAt(groupEvent.RawEvent);
        var userUrl = $"https://osu.ppy.sh/users/{groupEvent.UserId}";
        var groupUrl = $"https://osu.ppy.sh/groups/{groupEvent.GroupId}";

        return new FeedEventViewEntry(
            groupEvent.EventId,
            FeedType.Group,
            groupEvent.EventType,
            createdAt,
            userUrl,
            new FeedEventActor(
                groupEvent.UserId,
                userName,
                userProfile?.AvatarUrl,
                userProfile?.Badge),
            null,
            new FeedGroupEventViewData(
                groupEvent.UserId,
                userName,
                groupEvent.GroupId,
                groupName,
                playmodes,
                userUrl,
                groupUrl));
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
            // Ignore malformed raw payload.
        }

        return null;
    }

    private static string? TryGetMode(string rawEvent)
    {
        try
        {
            var root = JsonNode.Parse(rawEvent) as JsonObject;
            if (root?["comment"]?["modes"] is JsonArray modes)
            {
                foreach (var modeNode in modes)
                {
                    if (modeNode is null)
                        continue;

                    var modeValue = modeNode.ToString();
                    if (FeedEnumExtensions.TryParseRuleset(modeValue, out var rulesetFromComment))
                        return rulesetFromComment.ToCommandValue();
                }
            }

            var mode = root?.TryGetNestedString("beatmap", "mode")
                ?? root?.TryGetString("mode");

            if (FeedEnumExtensions.TryParseRuleset(mode, out var ruleset))
                return ruleset.ToCommandValue();

            var modeInt = root?.TryGetNestedInt64("beatmap", "mode_int")
                ?? root?.TryGetInt64("mode_int", "ruleset_id");

            if (FeedEnumExtensions.TryParseRulesetId(modeInt, out var parsedFromModeInt))
                return parsedFromModeInt.ToCommandValue();

            return null;
        }
        catch
        {
            return null;
        }
    }

    private static long? TryGetDiscussionPostId(string rawEvent)
    {
        try
        {
            var root = JsonNode.Parse(rawEvent) as JsonObject;
            return root?.TryGetInt64("discussion_post_id", "post_id")
                ?? root?.TryGetNestedInt64("comment", "beatmap_discussion_post_id")
                ?? root?.TryGetNestedInt64("discussion", "starting_post", "id");
        }
        catch
        {
            return null;
        }
    }

    private static long? TryGetDiscussionId(string rawEvent)
    {
        try
        {
            var root = JsonNode.Parse(rawEvent) as JsonObject;
            return root?.TryGetNestedInt64("comment", "beatmap_discussion_id")
                ?? root?.TryGetNestedInt64("discussion", "id");
        }
        catch
        {
            return null;
        }
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

    private async Task<IReadOnlyList<FeedMapHistoryAction>> BuildRankedHistoryAsync(
        BeatmapsetEvent beatmapsetEvent,
        CancellationToken cancellationToken)
    {
        if (beatmapsetEvent.EventType != FeedEventType.Ranked)
            return [];

        var completeHistory = await osuApiClient.GetCompleteBeatmapsetEventHistoryAsync(
            beatmapsetEvent.SetId,
            cancellationToken);

        var relevantHistory = completeHistory
            .Where(x => x.Id <= beatmapsetEvent.EventId)
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

        var actions = new List<FeedMapHistoryAction>();
        foreach (var historyEvent in trimmedHistory)
        {
            string? userName = null;
            if (historyEvent.UserId is not null)
                userName = await osuApiClient.GetUserNameAsync(historyEvent.UserId.Value, cancellationToken);

            actions.Add(new FeedMapHistoryAction(
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

            // Last nomination often immediately triggers qualification; represent it once as qualification.
            if (entry.Type == FeedEventType.Qualification &&
                previous.Type == FeedEventType.Nomination &&
                IsLikelyLinkedNominationAndQualification(previous, entry))
            {
                coalesced[^1] = entry;
                continue;
            }

            // Collapse duplicate mirrored/reset entries into a single visible item.
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

        // Multiple nominations are valid history and should stay.
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

    private sealed record RankedHistoryEvent(OsuBeatmapsetEventsEvent Event, FeedEventType Type);
    private sealed record RankedHistoryEntry(OsuBeatmapsetEventsEvent Event, FeedEventType Type, long? UserId);

    private async Task<IReadOnlyList<string>> ResolveModesAsync(
        BeatmapsetEvent beatmapsetEvent,
        DateTimeOffset? createdAt,
        CancellationToken cancellationToken)
    {
        var normalizedRulesets = FeedEnumExtensions.DeserializeRulesets(beatmapsetEvent.Rulesets);
        if (normalizedRulesets is not null && normalizedRulesets.Count > 0)
            return normalizedRulesets
                .OrderBy(x => x)
                .Select(x => x.ToCommandValue())
                .ToList();

        var directModes = TryGetModes(beatmapsetEvent.RawEvent);
        if (directModes.Count > 0)
            return directModes;

        await using var db = await dbContextFactory.CreateDbContextAsync(cancellationToken);
        var relatedEvents = await db.BeatmapsetEvents
            .Where(x =>
                x.SetId == beatmapsetEvent.SetId &&
                x.EventId < beatmapsetEvent.EventId)
            .OrderByDescending(x => x.EventId)
            .Take(10)
            .ToListAsync(cancellationToken);

        foreach (var relatedEvent in relatedEvents)
        {
            var relatedRulesets = FeedEnumExtensions.DeserializeRulesets(relatedEvent.Rulesets);
            if (relatedRulesets is not null && relatedRulesets.Count > 0)
            {
                return relatedRulesets
                    .OrderBy(x => x)
                    .Select(x => x.ToCommandValue())
                    .ToList();
            }

            var relatedModes = TryGetModes(relatedEvent.RawEvent);
            if (relatedModes.Count > 0)
                return relatedModes;
        }

        if (beatmapsetEvent.EventType is not (
                FeedEventType.Nomination
                or FeedEventType.Qualification
                or FeedEventType.Disqualification))
            return ["osu"];

        var apiModes = await osuApiClient.GetBeatmapsetModesFailsafeAsync(
            beatmapsetEvent.SetId,
            beatmapsetEvent.TriggeredBy,
            createdAt,
            cancellationToken);
        if (apiModes.Count > 0)
            return apiModes;

        return ["osu"];
    }

    private static List<string> TryGetModes(string rawEvent)
    {
        var modes = new List<string>();

        try
        {
            var root = JsonNode.Parse(rawEvent) as JsonObject;
            if (root?["comment"]?["modes"] is JsonArray commentModes)
            {
                foreach (var modeNode in commentModes)
                {
                    if (modeNode is null)
                        continue;

                    var rawMode = modeNode.ToString();
                    if (!FeedEnumExtensions.TryParseRuleset(rawMode, out var parsed))
                        continue;

                    var mode = parsed.ToCommandValue();
                    if (!modes.Contains(mode))
                        modes.Add(mode);
                }
            }

            if (modes.Count == 0)
            {
                var mode = TryGetMode(rawEvent);
                if (!string.IsNullOrWhiteSpace(mode))
                    modes.Add(mode);
            }
        }
        catch
        {
            // Ignore malformed payload.
        }

        return modes;
    }

    private static List<string> ParsePlaymodes(string? playmodes)
    {
        if (string.IsNullOrWhiteSpace(playmodes))
            return [];

        return playmodes
            .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Where(x => !string.IsNullOrWhiteSpace(x))
            .Select(x => x.ToLowerInvariant())
            .Distinct()
            .ToList();
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

    private static string? TryGetGroupName(string rawEvent)
    {
        try
        {
            var root = JsonNode.Parse(rawEvent) as JsonObject;
            return root?.TryGetString("group_name");
        }
        catch
        {
            return null;
        }
    }

    private static List<string> TryGetGroupPlaymodes(string rawEvent)
    {
        try
        {
            var root = JsonNode.Parse(rawEvent) as JsonObject;
            if (root?["playmodes"] is not JsonArray playmodesArray)
                return [];

            var modes = new List<string>();

            foreach (var modeNode in playmodesArray)
            {
                if (modeNode is null)
                    continue;

                var rawMode = modeNode.ToString();
                if (FeedEnumExtensions.TryParseRuleset(rawMode, out var ruleset))
                {
                    modes.Add(ruleset.ToCommandValue());
                    continue;
                }

                if (!string.IsNullOrWhiteSpace(rawMode))
                    modes.Add(rawMode.ToLowerInvariant());
            }

            return modes
                .Distinct()
                .ToList();
        }
        catch
        {
            return [];
        }
    }

    private static string Trim(string value, int maxLength)
    {
        if (value.Length <= maxLength)
            return value;

        return value[..Math.Max(0, maxLength - 3)] + "...";
    }

    private static string BuildMapCacheKey(BeatmapsetEvent beatmapsetEvent)
    {
        return string.Join(':',
            "feed_view",
            "map",
            beatmapsetEvent.EventId,
            beatmapsetEvent.TriggeredBy?.ToString() ?? "none",
            beatmapsetEvent.PostId?.ToString() ?? "none",
            beatmapsetEvent.DiscussionId?.ToString() ?? "none",
            beatmapsetEvent.CreatedAt?.ToUnixTimeSeconds().ToString() ?? "none",
            beatmapsetEvent.MapperUserId?.ToString() ?? "none",
            beatmapsetEvent.Rulesets ?? "none",
            BuildCacheToken(beatmapsetEvent.Message));
    }

    private static string BuildGroupCacheKey(GroupEvent groupEvent)
    {
        return string.Join(':',
            "feed_view",
            "group",
            groupEvent.EventId,
            groupEvent.EventType,
            groupEvent.CreatedAt?.ToUnixTimeSeconds().ToString() ?? "none",
            groupEvent.GroupName ?? "none",
            groupEvent.UserName ?? "none",
            groupEvent.Playmodes ?? "none");
    }

    private static string BuildCacheToken(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
            return "none";

        return value.GetHashCode().ToString(CultureInfo.InvariantCulture);
    }
}
