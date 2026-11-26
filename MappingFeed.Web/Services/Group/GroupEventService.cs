using System.Globalization;
using System.Text.Json.Nodes;
using MappingFeed.Common.Data.Entities;
using MappingFeed.Common.Data.Enums;
using MappingFeed.Common.Data.TransitionalRecords;
using MappingFeed.Common.Json;
using MappingFeed.Common.Repositories.Group;
using MappingFeed.Common.Services.Group;
using MappingFeed.Common.Services.Osu;
using MappingFeed.Common.Extensions;

namespace MappingFeed.Web.Services.Group;

public sealed class GroupEventService(
    IGroupEventRepository repository,
    IOsuApiService osuApiService) : IGroupEventService
{
    private const int DefaultLimit = 20;
    private const int MaxLimit = 100;

    public async Task<IReadOnlyList<GroupEvent>> SaveNewEventsAsync(
        IReadOnlyCollection<GroupEvent> events,
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

    public Task<IReadOnlyList<GroupEvent>> GetPendingEventsAsync(
        long afterEventId,
        int take,
        CancellationToken cancellationToken)
    {
        return repository.GetPendingEventsAsync(afterEventId, take, cancellationToken);
    }

    public Task<List<GroupEvent>> QueryRecentAsync(
        int take,
        long? beforeEventId,
        GroupEventsFilter filters,
        CancellationToken cancellationToken)
    {
        return repository.QueryRecentAsync(take, beforeEventId, filters, cancellationToken);
    }

    public async Task<FeedCursorPage<FeedEventViewEntry>> GetRecentEventsPageAsync(
        int? limit,
        long? beforeEventId,
        GroupEventsFilter filters,
        CancellationToken cancellationToken)
    {
        var take = Math.Clamp(limit ?? DefaultLimit, 1, MaxLimit);
        var rows = await QueryRecentAsync(take + 1, beforeEventId, filters, cancellationToken);
        var hasMore = rows.Count > take;
        var pageRows = rows.Take(take).ToList();
        var items = await Task.WhenAll(pageRows.Select(x => CreateViewEntryAsync(x, cancellationToken)));
        var nextCursor = hasMore && pageRows.Count > 0
            ? pageRows[^1].EventId.ToString()
            : null;

        return new FeedCursorPage<FeedEventViewEntry>(items, nextCursor);
    }

    public async Task<FeedEventViewEntry> CreateViewEntryAsync(GroupEvent groupEvent, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var userName = groupEvent.UserName.NullIfWhiteSpace() ?? $"User {groupEvent.UserId}";
        var groupName = groupEvent.GroupName.NullIfWhiteSpace()
            ?? TryGetGroupName(groupEvent.RawEvent)
            ?? $"Group {groupEvent.GroupId}";
        var groupColor = groupEvent.GroupColor.NullIfWhiteSpace()
            ?? TryGetGroupColor(groupEvent.RawEvent);

        var playmodes = ParsePlaymodes(groupEvent.Playmodes);
        if (playmodes.Count == 0)
            playmodes = await ResolvePlaymodesAsync(groupEvent, cancellationToken);

        var createdAt = groupEvent.CreatedAt ?? TryGetCreatedAt(groupEvent.RawEvent);
        var userUrl = $"https://osu.ppy.sh/users/{groupEvent.UserId}";
        var groupUrl = $"https://osu.ppy.sh/groups/{groupEvent.GroupId}";
        var actorColor = groupEvent.ActorColor.NullIfWhiteSpace()
            ?? TryGetActorColor(groupEvent.RawEvent);

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
                groupEvent.ActorBadge,
                actorColor),
            null,
            new FeedGroupEventViewData(
                groupEvent.UserId,
                userName,
                groupEvent.GroupId,
                groupName,
                groupColor,
                playmodes,
                userUrl,
                groupUrl));
    }

    private async Task<List<string>> ResolvePlaymodesAsync(GroupEvent groupEvent, CancellationToken cancellationToken)
    {
        var parsedFromRawEvent = TryGetGroupPlaymodes(groupEvent.RawEvent);
        if (parsedFromRawEvent.Count > 0)
            return parsedFromRawEvent;

        var fromApi = await osuApiService.GetUserGroupPlaymodesAsync(
            groupEvent.UserId,
            groupEvent.GroupId,
            cancellationToken);

        return fromApi
            .Where(x => !string.IsNullOrWhiteSpace(x))
            .Select(x => x.ToLowerInvariant())
            .Distinct()
            .ToList();
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

                string? rawMode;
                
                if (modeNode is JsonValue jsonValue)
                {
                    rawMode = jsonValue.GetValue<object>().ToString();
                }
                else
                {
                    rawMode = modeNode.ToString();
                }

                if (string.IsNullOrWhiteSpace(rawMode))
                    continue;

                if (FeedEnumExtensions.TryParseRuleset(rawMode, out var ruleset))
                    modes.Add(ruleset.ToCommandValue());
                else if (long.TryParse(rawMode, out var modeId) && FeedEnumExtensions.TryParseRulesetId(modeId, out var rulesetFromId))
                    modes.Add(rulesetFromId.ToCommandValue());
                else if (!string.IsNullOrWhiteSpace(rawMode))
                    modes.Add(rawMode.ToLowerInvariant());
            }

            return modes.Distinct().ToList();
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
            return DateTimeOffset.TryParse((string?)createdAtRaw, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal, out var createdAt)
                ? createdAt
                : null;
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

    private static string? TryGetGroupColor(string rawEvent)
    {
        try
        {
            var root = JsonNode.Parse(rawEvent) as JsonObject;
            var rawColor = root?.TryGetString("group_color", "group_colour")
                ?? root?.TryGetNestedString("group", "color")
                ?? root?.TryGetNestedString("group", "colour");

            return rawColor.NormalizeHexColor();
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

            var normalized = rawColor.NormalizeHexColor();
            if (!string.IsNullOrWhiteSpace(normalized))
                return normalized;

            if (root?["user"]?["groups"] is not JsonArray groups)
                return null;

            foreach (var group in groups.OfType<JsonObject>())
            {
                var groupColor = group.TryGetString("color", "colour").NormalizeHexColor();
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
}
