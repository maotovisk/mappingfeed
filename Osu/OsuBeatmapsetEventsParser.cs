using System.Text.Json;
using System.Text.Json.Nodes;

namespace MappingFeed.Osu;

public sealed record OsuBeatmapsetEventsPayload(
    IReadOnlyList<OsuBeatmapsetEventsEvent> Events,
    IReadOnlyList<OsuBeatmapsetEventsUser> Users,
    int? ReviewsMaxBlocks);

public sealed record OsuBeatmapsetEventsEvent(
    long Id,
    string Type,
    long BeatmapsetId,
    long? UserId,
    DateTimeOffset? CreatedAt,
    long? DiscussionId,
    long? DiscussionPostId,
    string? Message,
    string RawJson);

public sealed record OsuBeatmapsetEventsUser(
    long Id,
    string Username);

internal static class OsuBeatmapsetEventsParser
{
    private static readonly JsonSerializerOptions RawJsonOptions = new(JsonSerializerDefaults.Web);

    public static OsuBeatmapsetEventsPayload Parse(JsonObject root)
    {
        var events = ParseEvents(root);
        var users = ParseUsers(root);
        var reviewsMaxBlocksRaw = root.TryGetNestedInt64("reviewsConfig", "max_blocks");

        return new OsuBeatmapsetEventsPayload(
            events,
            users,
            reviewsMaxBlocksRaw is null ? null : (int)Math.Clamp(reviewsMaxBlocksRaw.Value, int.MinValue, int.MaxValue));
    }

    private static IReadOnlyList<OsuBeatmapsetEventsEvent> ParseEvents(JsonObject root)
    {
        var events = new List<OsuBeatmapsetEventsEvent>();

        foreach (var eventNode in (root.TryGetArray("events") ?? []).OfType<JsonObject>())
        {
            var parsed = ParseEvent(eventNode);
            if (parsed is not null)
                events.Add(parsed);
        }

        return events;
    }

    private static OsuBeatmapsetEventsEvent? ParseEvent(JsonObject eventNode)
    {
        var id = eventNode.TryGetInt64("id");
        var type = eventNode.TryGetString("type", "event_type");
        var beatmapsetId = eventNode.TryGetNestedInt64("beatmapset", "id")
            ?? eventNode.TryGetInt64("beatmapset_id");

        if (id is null || string.IsNullOrWhiteSpace(type) || beatmapsetId is null)
            return null;

        var createdAtRaw = eventNode.TryGetString("created_at");
        DateTimeOffset? createdAt = null;
        if (!string.IsNullOrWhiteSpace(createdAtRaw) &&
            DateTimeOffset.TryParse(createdAtRaw, out var parsedCreatedAt))
        {
            createdAt = parsedCreatedAt;
        }

        var discussionId = eventNode.TryGetNestedInt64("discussion", "id")
            ?? eventNode.TryGetNestedInt64("beatmap_discussion", "id")
            ?? eventNode.TryGetNestedInt64("comment", "beatmap_discussion_id");

        var discussionPostId = eventNode.TryGetNestedInt64("comment", "beatmap_discussion_post_id")
            ?? eventNode.TryGetNestedInt64("discussion", "starting_post", "id")
            ?? eventNode.TryGetNestedInt64("beatmap_discussion", "starting_post", "id");

        var message = eventNode.TryGetNestedString("discussion", "starting_post", "message")
            ?? eventNode.TryGetNestedString("beatmap_discussion", "starting_post", "message")
            ?? TryGetCommentString(eventNode["comment"]);

        return new OsuBeatmapsetEventsEvent(
            id.Value,
            type,
            beatmapsetId.Value,
            eventNode.TryGetInt64("user_id"),
            createdAt,
            discussionId,
            discussionPostId,
            message,
            eventNode.ToJsonString(RawJsonOptions));
    }

    private static IReadOnlyList<OsuBeatmapsetEventsUser> ParseUsers(JsonObject root)
    {
        var users = new List<OsuBeatmapsetEventsUser>();

        foreach (var userNode in (root.TryGetArray("users") ?? []).OfType<JsonObject>())
        {
            var id = userNode.TryGetInt64("id", "user_id");
            var username = userNode.TryGetString("username");

            if (id is null || string.IsNullOrWhiteSpace(username))
                continue;

            users.Add(new OsuBeatmapsetEventsUser(id.Value, username));
        }

        return users;
    }

    private static string? TryGetCommentString(JsonNode? commentNode)
    {
        if (commentNode is not JsonValue commentValue)
            return null;

        try
        {
            var comment = commentValue.GetValue<string>();
            return string.IsNullOrWhiteSpace(comment) ? null : comment;
        }
        catch
        {
            return null;
        }
    }
}
