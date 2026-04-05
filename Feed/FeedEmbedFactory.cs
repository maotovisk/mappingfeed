using MappingFeed.Data;
using MappingFeed.Data.Entities;
using MappingFeed.Osu;
using Microsoft.EntityFrameworkCore;
using NetCord;
using NetCord.Rest;
using System.Globalization;
using System.Text.Json.Nodes;

namespace MappingFeed.Feed;

public sealed class FeedEmbedFactory(
    OsuApiClient osuApiClient,
    IDbContextFactory<MappingFeedDbContext> dbContextFactory)
{
    public async Task<EmbedProperties> CreateBeatmapsetEventEmbedAsync(
        BeatmapsetEvent beatmapsetEvent,
        CancellationToken cancellationToken)
    {
        var beatmapset = await osuApiClient.GetBeatmapsetAsync(beatmapsetEvent.SetId, cancellationToken);
        var actorProfile = beatmapsetEvent.TriggeredBy is null
            ? null
            : await osuApiClient.GetUserProfileAsync(beatmapsetEvent.TriggeredBy.Value, cancellationToken);
        var actorName = actorProfile?.Username;
        var createdAt = TryGetCreatedAt(beatmapsetEvent.RawEvent);
        var discussionPostId = beatmapsetEvent.PostId ?? TryGetDiscussionPostId(beatmapsetEvent.RawEvent);
        var discussionId = TryGetDiscussionId(beatmapsetEvent.RawEvent);
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
            ShouldUseMessageFailsafe(beatmapsetEvent.EventType) &&
            beatmapsetEvent.TriggeredBy is not null)
        {
            resolvedMessage = await osuApiClient.GetLatestDiscussionMessageByUserAsync(
                beatmapsetEvent.SetId,
                beatmapsetEvent.TriggeredBy.Value,
                createdAt,
                cancellationToken);
        }

        var modeTags = await ResolveModeTagsAsync(beatmapsetEvent, createdAt, cancellationToken);
        var mapTitle = beatmapset?.Title ?? $"Beatmapset {beatmapsetEvent.SetId}";
        var mapper = beatmapset?.Creator ?? "Unknown";
        var mapperId = TryGetMapperUserId(beatmapsetEvent.RawEvent);
        var beatmapsetUrl = $"https://osu.ppy.sh/beatmapsets/{beatmapsetEvent.SetId}";
        var eventName = $"{GetEventEmoji(beatmapsetEvent.EventType)}  {GetMapEventDisplayName(beatmapsetEvent.EventType)} ({ToDiscordRelative(createdAt)})";
        var mapperLink = mapperId is null
            ? mapper
            : $"[{mapper}](https://osu.ppy.sh/users/{mapperId.Value})";
        var historyLine = await BuildMapHistoryLineAsync(beatmapsetEvent, cancellationToken);
        var footerText = BuildMapFooterText(beatmapsetEvent.EventType, actorName, actorProfile?.Badge, resolvedMessage);

        var valueLines = new List<string>
        {
            $"**[{mapTitle}]({beatmapsetUrl})**",
            $"Mapped by {mapperLink} **{modeTags}**",
        };

        var messageLine = BuildMapMessageLine(beatmapsetEvent.EventType, resolvedMessage);
        if (!string.IsNullOrWhiteSpace(messageLine))
            valueLines.Add(messageLine);

        if (!string.IsNullOrWhiteSpace(historyLine))
            valueLines.Add($"\n{historyLine}");

        var embed = new EmbedProperties()
            .WithColor(GetColor(beatmapsetEvent.EventType))
            .WithFields([
                new EmbedFieldProperties()
                    .WithName(eventName)
                    .WithValue(string.Join('\n', valueLines))
            ]);

        embed.WithThumbnail(new EmbedThumbnailProperties(BuildBeatmapsetThumbnailUrl(beatmapsetEvent.SetId)));

        if (!string.IsNullOrWhiteSpace(footerText))
        {
            var footer = new EmbedFooterProperties().WithText(footerText!);
            if (!string.IsNullOrWhiteSpace(actorProfile?.AvatarUrl))
                footer.WithIconUrl(actorProfile!.AvatarUrl!);

            embed.WithFooter(footer);
        }

        return embed;
    }

    public async Task<EmbedProperties> CreateGroupEventEmbedAsync(
        GroupEvent groupEvent,
        CancellationToken cancellationToken)
    {
        var userProfile = await osuApiClient.GetUserProfileAsync(groupEvent.UserId, cancellationToken);
        var userName = userProfile?.Username ?? await osuApiClient.GetUserNameAsync(groupEvent.UserId, cancellationToken) ?? $"User {groupEvent.UserId}";
        var groupName = TryGetGroupName(groupEvent.RawEvent)
            ?? await osuApiClient.GetGroupNameAsync(groupEvent.GroupId, cancellationToken)
            ?? $"Group {groupEvent.GroupId}";
        var playmodes = TryGetGroupPlaymodes(groupEvent.RawEvent);
        var createdAt = TryGetCreatedAt(groupEvent.RawEvent);
        var userLine = groupEvent.EventType switch
        {
            FeedEventType.GroupAdd => $"[{userName}](https://osu.ppy.sh/users/{groupEvent.UserId}) to the",
            FeedEventType.GroupRemove => $"[{userName}](https://osu.ppy.sh/users/{groupEvent.UserId}) from the",
            _ => $"[{userName}](https://osu.ppy.sh/users/{groupEvent.UserId}) in",
        };
        var groupLine = $"[**{groupName}**](https://osu.ppy.sh/groups/{groupEvent.GroupId})";

        var embed = new EmbedProperties()
            .WithDescription(BuildGroupDescription(groupEvent.EventType, userLine, groupLine, playmodes, createdAt))
            .WithColor(GetColor(groupEvent.EventType));

        if (!string.IsNullOrWhiteSpace(userProfile?.AvatarUrl))
            embed.WithThumbnail(new EmbedThumbnailProperties(userProfile!.AvatarUrl!));

        return embed;
    }

    private static Color GetColor(FeedEventType eventType) => eventType switch
    {
        FeedEventType.Nomination => new Color(52, 152, 219),
        FeedEventType.Qualification => new Color(255, 89, 120),
        FeedEventType.Ranked => new Color(241, 196, 15),
        FeedEventType.NominationReset => new Color(243, 156, 18),
        FeedEventType.Unranked => new Color(230, 126, 34),
        FeedEventType.Disqualification => new Color(231, 76, 60),
        FeedEventType.GroupAdd => new Color(87, 242, 135),
        FeedEventType.GroupRemove => new Color(201, 164, 255),
        FeedEventType.GroupMove => new Color(52, 152, 219),
        _ => new Color(149, 165, 166),
    };

    private static string GetEventEmoji(FeedEventType eventType) => eventType switch
    {
        FeedEventType.Nomination => "💬",
        FeedEventType.NominationReset => "↩️",
        FeedEventType.Qualification => "❤️",
        FeedEventType.Disqualification => "💔",
        FeedEventType.Ranked => "💖",
        FeedEventType.Unranked => "🟠",
        FeedEventType.GroupAdd => "👾",
        FeedEventType.GroupRemove => "👾",
        FeedEventType.GroupMove => "👥",
        _ => "•",
    };

    private static string ToDiscordRelative(DateTimeOffset? createdAt)
    {
        var timestamp = createdAt ?? DateTimeOffset.UtcNow;
        return $"<t:{timestamp.ToUnixTimeSeconds()}:R>";
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

            return modeInt switch
            {
                0 => Ruleset.Osu.ToCommandValue(),
                1 => Ruleset.Taiko.ToCommandValue(),
                2 => Ruleset.Catch.ToCommandValue(),
                3 => Ruleset.Mania.ToCommandValue(),
                _ => null,
            };
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

    private static string BuildGroupDescription(
        FeedEventType eventType,
        string userLine,
        string groupLine,
        string? playmodes,
        DateTimeOffset? createdAt)
    {
        var label = eventType switch
        {
            FeedEventType.GroupAdd => "Added",
            FeedEventType.GroupRemove => "Removed",
            _ => eventType.ToDisplayName(),
        };

        var lines = new List<string>
        {
            $"{GetEventEmoji(eventType)} **{label}** ({ToDiscordRelative(createdAt)})",
            userLine,
            groupLine,
        };

        if (!string.IsNullOrWhiteSpace(playmodes))
            lines.Add($"for [{playmodes}]");

        return string.Join('\n', lines);
    }

    private static string GetMapEventDisplayName(FeedEventType eventType) => eventType switch
    {
        FeedEventType.Nomination => "Nominated",
        FeedEventType.Qualification => "Qualified",
        FeedEventType.Disqualification => "Disqualified",
        FeedEventType.NominationReset => "Nomination Reset",
        FeedEventType.Ranked => "Ranked",
        FeedEventType.Unranked => "Unranked",
        _ => eventType.ToDisplayName(),
    };

    private static string? BuildMapMessageLine(FeedEventType eventType, string? message)
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

    private static string? BuildMapFooterText(
        FeedEventType eventType,
        string? actorName,
        string? actorBadge,
        string? message)
    {
        if (string.IsNullOrWhiteSpace(actorName))
            return null;

        var baseText = string.IsNullOrWhiteSpace(actorBadge)
            ? actorName!.Trim()
            : $"{actorName!.Trim()} [{actorBadge.Trim()}]";

        if (eventType is FeedEventType.NominationReset or FeedEventType.Disqualification &&
            !string.IsNullOrWhiteSpace(message))
        {
            return $"{baseText} - \"{Trim(message.Trim(), 53)}\"";
        }

        return eventType switch
        {
            FeedEventType.Nomination => baseText,
            FeedEventType.Qualification => baseText,
            FeedEventType.Disqualification => baseText,
            FeedEventType.NominationReset => baseText,
            _ => null,
        };
    }

    private async Task<string?> BuildMapHistoryLineAsync(
        BeatmapsetEvent beatmapsetEvent,
        CancellationToken cancellationToken)
    {
        if (beatmapsetEvent.EventType != FeedEventType.Ranked)
            return null;

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
            return null;

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

        var parts = new List<string>();
        foreach (var historyEvent in trimmedHistory)
        {
            if (historyEvent.UserId is null)
                continue;

            var userName = await osuApiClient.GetUserNameAsync(historyEvent.UserId.Value, cancellationToken);
            if (string.IsNullOrWhiteSpace(userName))
                continue;

            parts.Add($"{GetEventEmoji(historyEvent.Type)} {userName}");
        }

        return parts.Count == 0 ? null : string.Join("  ", parts);
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

    private static string BuildBeatmapsetThumbnailUrl(long setId)
    {
        return $"https://b.ppy.sh/thumb/{setId}l.jpg";
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

    private async Task<string> ResolveModeTagsAsync(
        BeatmapsetEvent beatmapsetEvent,
        DateTimeOffset? createdAt,
        CancellationToken cancellationToken)
    {
        var directTags = TryGetModeTags(beatmapsetEvent.RawEvent);
        if (directTags.Count > 0)
            return string.Join(string.Empty, directTags);

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
            var relatedTags = TryGetModeTags(relatedEvent.RawEvent);
            if (relatedTags.Count > 0)
                return string.Join(string.Empty, relatedTags);
        }

        if (!ShouldUseModeFailsafe(beatmapsetEvent.EventType))
            return "[osu]";

        var apiModes = await osuApiClient.GetBeatmapsetModesFailsafeAsync(
            beatmapsetEvent.SetId,
            beatmapsetEvent.TriggeredBy,
            createdAt,
            cancellationToken);
        if (apiModes.Count > 0)
            return string.Join(string.Empty, apiModes.Select(x => $"[{x}]"));

        return "[osu]";
    }

    private static List<string> TryGetModeTags(string rawEvent)
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

                    var tag = $"[{parsed.ToCommandValue()}]";
                    if (!modes.Contains(tag))
                        modes.Add(tag);
                }
            }

            if (modes.Count == 0)
            {
                var mode = TryGetMode(rawEvent);
                if (!string.IsNullOrWhiteSpace(mode))
                    modes.Add($"[{mode}]");
            }
        }
        catch
        {
            // Ignore malformed payload.
        }

        return modes;
    }

    private static bool ShouldUseModeFailsafe(FeedEventType eventType)
    {
        return eventType is FeedEventType.Nomination
            or FeedEventType.Qualification
            or FeedEventType.Disqualification;
    }

    private static bool ShouldUseMessageFailsafe(FeedEventType eventType)
    {
        return eventType is FeedEventType.Nomination
            or FeedEventType.Qualification
            or FeedEventType.Disqualification;
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

    private static string? TryGetGroupPlaymodes(string rawEvent)
    {
        try
        {
            var root = JsonNode.Parse(rawEvent) as JsonObject;
            if (root?["playmodes"] is not JsonArray playmodesArray)
                return null;

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

            return modes.Count == 0 ? null : string.Join(", ", modes.Distinct());
        }
        catch
        {
            return null;
        }
    }

    private static string Trim(string value, int maxLength)
    {
        if (value.Length <= maxLength)
            return value;

        return value[..Math.Max(0, maxLength - 3)] + "...";
    }
}
