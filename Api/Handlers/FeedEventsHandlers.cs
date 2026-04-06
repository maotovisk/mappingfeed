using MappingFeed.Feed;
using Microsoft.AspNetCore.Http.HttpResults;

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
        FeedEventQueryService queryService,
        CancellationToken cancellationToken)
    {
        if (!TryParseEventIdCursor(cursor, out var beforeEventId, out var error))
            return TypedResults.BadRequest(error);

        var page = await queryService.GetRecentMapEventsPageAsync(limit, beforeEventId, cancellationToken);
        return TypedResults.Ok(ToPageResponse(FeedType.Map.ToCommandValue(), page));
    }

    private static async Task<Results<Ok<FeedEventsCursorPageResponse<FeedEventViewEntry>>, BadRequest<string>>> GetRecentGroupEventsAsync(
        int? limit,
        string? cursor,
        FeedEventQueryService queryService,
        CancellationToken cancellationToken)
    {
        if (!TryParseEventIdCursor(cursor, out var beforeEventId, out var error))
            return TypedResults.BadRequest(error);

        var page = await queryService.GetRecentGroupEventsPageAsync(limit, beforeEventId, cancellationToken);
        return TypedResults.Ok(ToPageResponse(FeedType.Group.ToCommandValue(), page));
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
