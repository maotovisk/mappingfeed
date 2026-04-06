namespace MappingFeed.Feed;

public sealed record FeedEventActor(
    long? UserId,
    string? Username,
    string? AvatarUrl,
    string? Badge);

public sealed record FeedMapHistoryAction(
    FeedEventType Action,
    long? UserId,
    string? Username);

public sealed record FeedMapEventViewData(
    long SetId,
    string BeatmapsetUrl,
    string BeatmapsetTitle,
    string MapperName,
    long? MapperUserId,
    IReadOnlyList<string> Modes,
    string? Message,
    IReadOnlyList<FeedMapHistoryAction> RankedHistory);

public sealed record FeedGroupEventViewData(
    long UserId,
    string UserName,
    long GroupId,
    string GroupName,
    IReadOnlyList<string> Playmodes,
    string UserUrl,
    string GroupUrl);

public sealed record FeedEventViewEntry(
    long EventId,
    FeedType FeedType,
    FeedEventType EventType,
    DateTimeOffset? CreatedAt,
    string PrimaryUrl,
    FeedEventActor? Actor,
    FeedMapEventViewData? Map,
    FeedGroupEventViewData? Group);
