namespace MappingFeed.Common.Data.TransitionalRecords;

public sealed record FeedCursorPage<T>(
    IReadOnlyList<T> Items,
    string? NextCursor);
