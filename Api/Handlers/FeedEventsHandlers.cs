using MappingFeed.Feed;
using Microsoft.AspNetCore.Http.HttpResults;
using Microsoft.AspNetCore.Mvc;

namespace MappingFeed.Api.Handlers;

public static class FeedEventsHandlers
{
    public static IEndpointRouteBuilder MapFeedEventsApi(this IEndpointRouteBuilder app)
    {
        var group = app.MapGroup("/api/events");

        group.MapGet("/map", GetRecentMapEventsAsync);
        group.MapGet("/group", GetRecentGroupEventsAsync);

        return app;
    }

    private static async Task<Results<Ok<FeedEventsCursorPageResponse<FeedEventViewEntry>>, BadRequest<string>>> GetRecentMapEventsAsync(
        int? limit,
        string? cursor,
        [FromQuery(Name = "ruleset")] string? ruleset,
        [FromQuery(Name = "event_type")] string[]? eventType,
        [FromQuery(Name = "text")] string? text,
        FeedEventQueryService queryService,
        CancellationToken cancellationToken)
    {
        if (!TryParseEventIdCursor(cursor, out var beforeEventId, out var error))
            return TypedResults.BadRequest(error);

        if (!TryParseMapFilters(ruleset, eventType, text, out var filters, out error))
            return TypedResults.BadRequest(error);

        var page = await queryService.GetRecentMapEventsPageAsync(limit, beforeEventId, filters, cancellationToken);
        return TypedResults.Ok(ToPageResponse(FeedType.Map.ToCommandValue(), page));
    }

    private static async Task<Results<Ok<FeedEventsCursorPageResponse<FeedEventViewEntry>>, BadRequest<string>>> GetRecentGroupEventsAsync(
        int? limit,
        string? cursor,
        [FromQuery(Name = "group_id")] string[]? groupId,
        [FromQuery(Name = "playmode")] string? playmode,
        FeedEventQueryService queryService,
        CancellationToken cancellationToken)
    {
        if (!TryParseEventIdCursor(cursor, out var beforeEventId, out var error))
            return TypedResults.BadRequest(error);

        if (!TryParseGroupFilters(groupId, playmode, out var filters, out error))
            return TypedResults.BadRequest(error);

        var page = await queryService.GetRecentGroupEventsPageAsync(limit, beforeEventId, filters, cancellationToken);
        return TypedResults.Ok(ToPageResponse(FeedType.Group.ToCommandValue(), page));
    }

    private static bool TryParseMapFilters(
        string? ruleset,
        string[]? eventTypes,
        string? text,
        out MapEventsFilter filters,
        out string error)
    {
        error = string.Empty;

        Ruleset? parsedRuleset = null;
        if (!string.IsNullOrWhiteSpace(ruleset))
        {
            if (!FeedEnumExtensions.TryParseRuleset(ruleset, out var value))
            {
                filters = default!;
                error = "Invalid ruleset. Expected one of: osu, taiko, catch, mania.";
                return false;
            }

            parsedRuleset = value;
        }

        HashSet<FeedEventType>? parsedEventTypes = null;
        if (eventTypes is { Length: > 0 })
        {
            parsedEventTypes = [];

            foreach (var token in eventTypes
                         .Where(x => !string.IsNullOrWhiteSpace(x))
                         .SelectMany(x => x.Split([',', '|'], StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)))
            {
                if (!FeedEnumExtensions.TryParseMapEventType(token, out var value))
                {
                    filters = default!;
                    error = "Invalid event_type for map feed.";
                    return false;
                }

                parsedEventTypes.Add(value);
            }

            if (parsedEventTypes.Count == 0)
                parsedEventTypes = null;
        }

        var normalizedText = string.IsNullOrWhiteSpace(text) ? null : text.Trim();
        filters = new MapEventsFilter(parsedRuleset, parsedEventTypes, normalizedText);
        return true;
    }

    private static bool TryParseGroupFilters(
        string[]? groupIds,
        string? playmode,
        out GroupEventsFilter filters,
        out string error)
    {
        error = string.Empty;

        HashSet<long>? parsedGroupIds = null;
        if (groupIds is { Length: > 0 })
        {
            parsedGroupIds = [];
            foreach (var token in groupIds
                         .Where(x => !string.IsNullOrWhiteSpace(x))
                         .SelectMany(x => x.Split([',', '|'], StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)))
            {
                if (!long.TryParse(token, out var value) || value <= 0)
                {
                    filters = default!;
                    error = "Invalid group_id: expected positive numbers.";
                    return false;
                }

                parsedGroupIds.Add(value);
            }

            if (parsedGroupIds.Count == 0)
                parsedGroupIds = null;
        }

        string? normalizedPlaymode = null;
        if (!string.IsNullOrWhiteSpace(playmode))
        {
            if (!FeedEnumExtensions.TryParseRuleset(playmode, out var ruleset))
            {
                filters = default!;
                error = "Invalid playmode. Expected one of: osu, taiko, catch, mania.";
                return false;
            }

            normalizedPlaymode = ruleset.ToCommandValue();
        }

        filters = new GroupEventsFilter(parsedGroupIds, normalizedPlaymode);
        return true;
    }

    private static bool TryParseEventIdCursor(string? cursor, out long? beforeEventId, out string error)
    {
        beforeEventId = null;
        error = string.Empty;

        if (string.IsNullOrWhiteSpace(cursor))
            return true;

        if (long.TryParse(cursor, out var parsed) && parsed > 0)
        {
            beforeEventId = parsed;
            return true;
        }

        error = "Invalid cursor: expected a positive event id.";
        return false;
    }

    private static FeedEventsCursorPageResponse<TItem> ToPageResponse<TItem>(string feed, FeedCursorPage<TItem> page) =>
        new(
            feed,
            page.Items.Count,
            page.NextCursor,
            page.Items);

    private sealed record FeedEventsCursorPageResponse<TItem>(
        string Feed,
        int Count,
        string? NextCursor,
        IReadOnlyList<TItem> Items);
}
