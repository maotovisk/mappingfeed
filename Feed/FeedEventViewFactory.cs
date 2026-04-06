using System.Globalization;
using System.Text.Json;
using System.Text.Json.Nodes;
using MappingFeed.Data.Entities;
using MappingFeed.Osu;

namespace MappingFeed.Feed;

public sealed class FeedEventViewFactory
{
    public Task<FeedEventViewEntry> CreateBeatmapsetEventEntryAsync(
        BeatmapsetEvent beatmapsetEvent,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return Task.FromResult(BuildBeatmapsetEventEntry(beatmapsetEvent));
    }

    public Task<FeedEventViewEntry> CreateGroupEventEntryAsync(
        GroupEvent groupEvent,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return Task.FromResult(BuildGroupEventEntry(groupEvent));
    }

    private static FeedEventViewEntry BuildBeatmapsetEventEntry(BeatmapsetEvent beatmapsetEvent)
    {
        var createdAt = beatmapsetEvent.CreatedAt ?? TryGetCreatedAt(beatmapsetEvent.RawEvent);
        var beatmapsetUrl = $"https://osu.ppy.sh/beatmapsets/{beatmapsetEvent.SetId}";
        var actorName = FirstNonEmpty(beatmapsetEvent.ActorUsername);
        var actor = HasActorData(beatmapsetEvent.TriggeredBy, actorName, beatmapsetEvent.ActorAvatarUrl, beatmapsetEvent.ActorBadge)
            ? new FeedEventActor(
                beatmapsetEvent.TriggeredBy,
                actorName,
                beatmapsetEvent.ActorAvatarUrl,
                beatmapsetEvent.ActorBadge)
            : null;

        var mapTitle = FirstNonEmpty(beatmapsetEvent.BeatmapsetTitle) ?? $"Beatmapset {beatmapsetEvent.SetId}";
        var mapperName = FirstNonEmpty(beatmapsetEvent.MapperName) ?? "Unknown";
        var mapperId = beatmapsetEvent.MapperUserId ?? TryGetMapperUserId(beatmapsetEvent.RawEvent);
        var modes = ResolveModes(beatmapsetEvent);
        var message = NormalizeMapMessage(beatmapsetEvent.EventType, beatmapsetEvent.Message);
        var rankedHistory = ParseRankedHistory(beatmapsetEvent.RankedHistoryJson);

        return new FeedEventViewEntry(
            beatmapsetEvent.EventId,
            FeedType.Map,
            beatmapsetEvent.EventType,
            createdAt,
            beatmapsetUrl,
            actor,
            new FeedMapEventViewData(
                beatmapsetEvent.SetId,
                beatmapsetUrl,
                mapTitle,
                mapperName,
                mapperId,
                modes,
                message,
                rankedHistory),
            null);
    }

    private static FeedEventViewEntry BuildGroupEventEntry(GroupEvent groupEvent)
    {
        var userName = FirstNonEmpty(groupEvent.UserName) ?? $"User {groupEvent.UserId}";
        var groupName = FirstNonEmpty(groupEvent.GroupName)
            ?? TryGetGroupName(groupEvent.RawEvent)
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
                groupEvent.ActorAvatarUrl,
                groupEvent.ActorBadge),
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

    private static IReadOnlyList<string> ResolveModes(BeatmapsetEvent beatmapsetEvent)
    {
        var normalizedRulesets = FeedEnumExtensions.DeserializeRulesets(beatmapsetEvent.Rulesets);
        if (normalizedRulesets is not null && normalizedRulesets.Count > 0)
        {
            return normalizedRulesets
                .OrderBy(x => x)
                .Select(x => x.ToCommandValue())
                .ToList();
        }

        var directModes = TryGetModes(beatmapsetEvent.RawEvent);
        if (directModes.Count > 0)
            return directModes;

        return ["osu"];
    }

    private static IReadOnlyList<FeedMapHistoryAction> ParseRankedHistory(string? rankedHistoryJson)
    {
        if (string.IsNullOrWhiteSpace(rankedHistoryJson))
            return [];

        try
        {
            var snapshots = JsonSerializer.Deserialize<List<RankedHistorySnapshot>>(rankedHistoryJson);
            if (snapshots is null || snapshots.Count == 0)
                return [];

            return snapshots
                .Select(x => new FeedMapHistoryAction(
                    x.Action,
                    x.UserId,
                    string.IsNullOrWhiteSpace(x.Username) ? null : x.Username))
                .ToList();
        }
        catch
        {
            return [];
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

    private static bool HasActorData(long? userId, string? username, string? avatarUrl, string? badge)
    {
        return userId is not null ||
               !string.IsNullOrWhiteSpace(username) ||
               !string.IsNullOrWhiteSpace(avatarUrl) ||
               !string.IsNullOrWhiteSpace(badge);
    }

    private static string? FirstNonEmpty(string? value)
    {
        return string.IsNullOrWhiteSpace(value) ? null : value;
    }

    private static string Trim(string value, int maxLength)
    {
        if (value.Length <= maxLength)
            return value;

        return value[..Math.Max(0, maxLength - 3)] + "...";
    }

    private sealed record RankedHistorySnapshot(
        FeedEventType Action,
        long? UserId,
        string? Username);
}
