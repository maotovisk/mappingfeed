using System.Globalization;
using System.Text.Json;
using System.Text.Json.Nodes;
using MappingFeed.Common.Config;
using MappingFeed.Common.Data.Entities;
using MappingFeed.Common.Data.Enums;
using MappingFeed.Common.Data.TransitionalRecords;
using MappingFeed.Common.Json;
using MappingFeed.Common.Services.Group;
using MappingFeed.Common.Services.Osu;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using MappingFeed.Common.Extensions;

namespace MappingFeed.Scraper.Fetchers.Events;

public sealed class GroupEventsFetcher(
    IGroupEventService groupEventService,
    IOsuApiService osuApiService,
    IOptions<FeedOptions> options,
    ILogger<GroupEventsFetcher> logger)
{
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);
    private readonly FeedOptions _options = options.Value;

    public async Task FetchAsync(CancellationToken cancellationToken)
    {
        await FetchGroupEventsAsync(cancellationToken);
    }

    private async Task FetchGroupEventsAsync(CancellationToken cancellationToken)
    {
        logger.LogInformation("Fetching group events.");
        var payloads = await osuApiService.GetGroupEventsAsync(_options.EventsBatchSize, cancellationToken);

        var parsedEvents = payloads
            .Select(ParseGroupEvent)
            .Where(x => x is not null)
            .Select(x => x!)
            .OrderBy(x => x.EventId)
            .ToList();

        if (parsedEvents.Count == 0)
            return;

        await EnrichGroupEventsAsync(parsedEvents, cancellationToken);

        var newEvents = await groupEventService.SaveNewEventsAsync(parsedEvents, cancellationToken);
        if (newEvents.Count == 0)
            return;

        logger.LogInformation("Fetched {Count} group events.", newEvents.Count);
    }

    private async Task EnrichGroupEventsAsync(
        IReadOnlyCollection<GroupEvent> parsedEvents,
        CancellationToken cancellationToken)
    {
        var profileCache = new Dictionary<long, OsuUserProfileInfo?>();
        var groupNameCache = new Dictionary<long, string?>();
        var groupColorCache = new Dictionary<long, string?>();

        foreach (var parsedEvent in parsedEvents.OrderBy(x => x.EventId))
        {
            if (!profileCache.TryGetValue(parsedEvent.UserId, out var profile))
            {
                profile = await osuApiService.GetUserAsync(parsedEvent.UserId, cancellationToken);
                profileCache[parsedEvent.UserId] = profile;
            }

            parsedEvent.UserName = parsedEvent.UserName.NullIfWhiteSpace()
                ?? profile?.Username.NullIfWhiteSpace()
                ?? await osuApiService.GetUserNameAsync(parsedEvent.UserId, cancellationToken)
                ?? $"User {parsedEvent.UserId}";
            parsedEvent.ActorAvatarUrl = parsedEvent.ActorAvatarUrl.NullIfWhiteSpace()
                ?? profile?.AvatarUrl.NullIfWhiteSpace();
            parsedEvent.ActorBadge = parsedEvent.ActorBadge.NullIfWhiteSpace()
                ?? profile?.Badge.NullIfWhiteSpace();
            parsedEvent.ActorColor = parsedEvent.ActorColor.NullIfWhiteSpace()
                ?? profile?.Color.NullIfWhiteSpace();

            if (!groupNameCache.TryGetValue(parsedEvent.GroupId, out var groupName))
            {
                groupName = await osuApiService.GetGroupNameAsync(parsedEvent.GroupId, cancellationToken);
                groupNameCache[parsedEvent.GroupId] = groupName;
            }

            if (!groupColorCache.TryGetValue(parsedEvent.GroupId, out var groupColor))
            {
                groupColor = await osuApiService.GetGroupColorAsync(parsedEvent.GroupId, cancellationToken);
                groupColorCache[parsedEvent.GroupId] = groupColor;
            }

            parsedEvent.GroupName = parsedEvent.GroupName.NullIfWhiteSpace()
                ?? groupName.NullIfWhiteSpace()
                ?? $"Group {parsedEvent.GroupId}";
            parsedEvent.GroupColor = parsedEvent.GroupColor.NullIfWhiteSpace()
                ?? groupColor.NullIfWhiteSpace();
        }
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
            ActorColor = TryExtractActorColor(payload),
            CreatedAt = TryParseCreatedAt(payload.TryGetString("created_at")),
            GroupId = groupId.Value,
            GroupName = payload.TryGetString("group_name")
                ?? payload.TryGetNestedString("group", "short_name")
                ?? payload.TryGetNestedString("group", "name"),
            GroupColor = TryExtractGroupColor(payload),
            Playmodes = TryExtractGroupPlaymodes(payload),
            EventType = eventType.Value,
            RawEvent = payload.ToJsonString(JsonOptions),
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

    private static string? TryExtractGroupColor(JsonObject payload)
    {
        var rawColor = payload.TryGetString("group_color", "group_colour")
            ?? payload.TryGetNestedString("group", "color")
            ?? payload.TryGetNestedString("group", "colour");

        return rawColor.NormalizeHexColor();
    }

    private static string? TryExtractActorColor(JsonObject payload)
    {
        var rawColor = payload.TryGetString("user_color", "user_colour")
            ?? payload.TryGetNestedString("user", "color")
            ?? payload.TryGetNestedString("user", "colour");

        var normalized = rawColor.NormalizeHexColor();
        if (!string.IsNullOrWhiteSpace(normalized))
            return normalized;

        var user = payload["user"] as JsonObject;
        if (user?["groups"] is not JsonArray groups)
            return null;

        foreach (var group in groups.OfType<JsonObject>())
        {
            var groupColor = group.TryGetString("color", "colour").NormalizeHexColor();
            if (!string.IsNullOrWhiteSpace(groupColor))
                return groupColor;
        }

        return null;
    }
}
