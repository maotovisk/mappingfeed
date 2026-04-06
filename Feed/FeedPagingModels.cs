namespace MappingFeed.Feed;

public sealed record FeedCursorPage<T>(
    IReadOnlyList<T> Items,
    string? NextCursor);
