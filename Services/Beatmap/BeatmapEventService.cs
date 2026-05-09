using System.Globalization;
using System.Text.Json;
using System.Text.Json.Nodes;
using MappingFeed.Data.Entities;
using MappingFeed.Repositories.Beatmap;

namespace MappingFeed.Services.Beatmap;

public sealed class BeatmapEventService(IBeatmapEventRepository repository) : IBeatmapEventService
{
    private const int DefaultLimit = 20;
    private const int MaxLimit = 100;

    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web)
    {
        PropertyNameCaseInsensitive = true,
    };

    public async Task<IReadOnlyList<BeatmapsetEvent>> SaveNewEventsAsync(
        IReadOnlyCollection<BeatmapsetEvent> events,
        CancellationToken cancellationToken)
    {
        if (events.Count == 0)
            return [];

        var incomingEventIds = events.Select(x => x.EventId).ToHashSet();
        var existingIdSet = await repository.GetExistingEventIdsAsync(incomingEventIds, cancellationToken);

        var newEvents = events
            .Where(x => !existingIdSet.Contains(x.EventId))
            .ToList();

        await repository.AddRangeAsync(newEvents, cancellationToken);
        return newEvents;
    }

    public Task<IReadOnlyList<BeatmapsetEvent>> GetPendingEventsAsync(
        long afterEventId,
        int take,
        CancellationToken cancellationToken)
    {
        return repository.GetPendingEventsAsync(afterEventId, take, cancellationToken);
    }

    public Task<bool> HasEarlierNominationAsync(
        long setId,
        long beforeEventId,
        CancellationToken cancellationToken)
    {
        return repository.HasEarlierNominationAsync(setId, beforeEventId, cancellationToken);
    }

    public Task UpdateAsync(BeatmapsetEvent beatmapsetEvent, CancellationToken cancellationToken)
    {
        return repository.UpdateAsync(beatmapsetEvent, cancellationToken);
    }

    public Task<List<BeatmapsetEvent>> QueryRecentAsync(
        int take,
        long? beforeEventId,
        MapEventsFilter filters,
        CancellationToken cancellationToken)
    {
        return repository.QueryRecentAsync(take, beforeEventId, filters, cancellationToken);
    }

    public async Task<FeedCursorPage<FeedEventViewEntry>> GetRecentEventsPageAsync(
        int? limit,
        long? beforeEventId,
        MapEventsFilter filters,
        CancellationToken cancellationToken)
    {
        var take = NormalizeLimit(limit);
        var rows = await QueryRecentAsync(take + 1, beforeEventId, filters, cancellationToken);
        var hasMore = rows.Count > take;
        var pageRows = rows.Take(take).ToList();
        var items = await Task.WhenAll(pageRows.Select(x => CreateViewEntryAsync(x, cancellationToken)));
        var nextCursor = hasMore && pageRows.Count > 0
            ? pageRows[^1].EventId.ToString()
            : null;

        return new FeedCursorPage<FeedEventViewEntry>(items, nextCursor);
    }

    public Task<FeedEventViewEntry> CreateViewEntryAsync(
        BeatmapsetEvent beatmapsetEvent,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var createdAt = beatmapsetEvent.CreatedAt ?? TryGetCreatedAt(beatmapsetEvent.RawEvent);
        var beatmapsetUrl = $"https://osu.ppy.sh/beatmapsets/{beatmapsetEvent.SetId}";
        var actorName = FirstNonEmpty(beatmapsetEvent.ActorUsername);
        var actorColor = FirstNonEmpty(beatmapsetEvent.ActorColor)
            ?? TryGetActorColor(beatmapsetEvent.RawEvent);
        var actor = HasActorData(beatmapsetEvent.TriggeredBy, actorName, beatmapsetEvent.ActorAvatarUrl, beatmapsetEvent.ActorBadge, actorColor)
            ? new FeedEventActor(
                beatmapsetEvent.TriggeredBy,
                actorName,
                beatmapsetEvent.ActorAvatarUrl,
                beatmapsetEvent.ActorBadge,
                actorColor)
            : null;

        var mapTitle = FirstNonEmpty(beatmapsetEvent.BeatmapsetTitle) ?? $"Beatmapset {beatmapsetEvent.SetId}";
        var mapperName = FirstNonEmpty(beatmapsetEvent.MapperName) ?? "Unknown";
        var mapperId = beatmapsetEvent.MapperUserId ?? TryGetMapperUserId(beatmapsetEvent.RawEvent);
        var modes = ResolveModes(beatmapsetEvent);
        var message = NormalizeMapMessage(beatmapsetEvent.EventType, beatmapsetEvent.Message);
        var rankedHistory = ParseRankedHistory(beatmapsetEvent.RankedHistoryJson);

        return Task.FromResult(new FeedEventViewEntry(
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
            null));
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
        return directModes.Count > 0 ? directModes : ["osu"];
    }

    private static IReadOnlyList<FeedMapHistoryAction> ParseRankedHistory(string? rankedHistoryJson)
    {
        if (string.IsNullOrWhiteSpace(rankedHistoryJson))
            return [];

        try
        {
            var snapshots = JsonSerializer.Deserialize<List<RankedHistorySnapshot>>(rankedHistoryJson, JsonOptions);
            return snapshots is null
                ? []
                : snapshots
                    .Select(x => new FeedMapHistoryAction(
                        x.Action,
                        x.UserId,
                        string.IsNullOrWhiteSpace(x.Username) ? null : x.Username,
                        FirstNonEmpty(x.UserColor)))
                    .ToList();
        }
        catch
        {
            return [];
        }
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

    private static string? TryGetMode(string rawEvent)
    {
        try
        {
            var root = JsonNode.Parse(rawEvent) as JsonObject;
            var mode = root?.TryGetNestedString("beatmap", "mode")
                ?? root?.TryGetString("mode");

            if (FeedEnumExtensions.TryParseRuleset(mode, out var ruleset))
                return ruleset.ToCommandValue();

            var modeInt = root?.TryGetNestedInt64("beatmap", "mode_int")
                ?? root?.TryGetInt64("mode_int", "ruleset_id");

            return FeedEnumExtensions.TryParseRulesetId(modeInt, out var parsedFromModeInt)
                ? parsedFromModeInt.ToCommandValue()
                : null;
        }
        catch
        {
            return null;
        }
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

    private static DateTimeOffset? TryGetCreatedAt(string rawEvent)
    {
        try
        {
            var root = JsonNode.Parse(rawEvent) as JsonObject;
            var createdAtRaw = root?.TryGetString("created_at");
            return DateTimeOffset.TryParse(createdAtRaw, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal, out var createdAt)
                ? createdAt
                : null;
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
            var rawColor = root?.TryGetString("user_color", "user_colour")
                ?? root?.TryGetNestedString("user", "color")
                ?? root?.TryGetNestedString("user", "colour");

            var normalized = NormalizeColor(rawColor);
            if (!string.IsNullOrWhiteSpace(normalized))
                return normalized;

            if (root?["user"]?["groups"] is not JsonArray groups)
                return null;

            foreach (var group in groups.OfType<JsonObject>())
            {
                var groupColor = NormalizeColor(group.TryGetString("color", "colour"));
                if (!string.IsNullOrWhiteSpace(groupColor))
                    return groupColor;
            }

            return null;
        }
        catch
        {
            return null;
        }
    }

    private static string? NormalizeMapMessage(FeedEventType eventType, string? message)
    {
        if (eventType is not (FeedEventType.Nomination or FeedEventType.Qualification or FeedEventType.Disqualification or FeedEventType.NominationReset))
            return null;

        if (string.IsNullOrWhiteSpace(message))
            return null;

        var normalized = string.Join(' ', message
            .Split(['\r', '\n'], StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries));

        return string.IsNullOrWhiteSpace(normalized) ? null : Trim(normalized, 220);
    }

    private static bool HasActorData(long? userId, string? username, string? avatarUrl, string? badge, string? color)
    {
        return userId is not null ||
               !string.IsNullOrWhiteSpace(username) ||
               !string.IsNullOrWhiteSpace(avatarUrl) ||
               !string.IsNullOrWhiteSpace(badge) ||
               !string.IsNullOrWhiteSpace(color);
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

        return trimmed.Length == 6 && trimmed.All(Uri.IsHexDigit)
            ? $"#{trimmed.ToUpperInvariant()}"
            : null;
    }

    private static string? FirstNonEmpty(string? value) => string.IsNullOrWhiteSpace(value) ? null : value;

    private static int NormalizeLimit(int? limit)
    {
        if (limit is null)
            return DefaultLimit;

        return Math.Clamp(limit.Value, 1, MaxLimit);
    }

    private static string Trim(string value, int maxLength)
    {
        return value.Length <= maxLength
            ? value
            : value[..Math.Max(0, maxLength - 3)] + "...";
    }

    private sealed record RankedHistorySnapshot(
        FeedEventType Action,
        long? UserId,
        string? Username,
        string? UserColor);
}
