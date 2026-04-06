namespace MappingFeed.Feed;

public sealed record FeedEventColor(byte R, byte G, byte B);

public sealed record FeedEventActor(
    long? UserId,
    string? Username,
    string? AvatarUrl,
    string? Badge);

public sealed record FeedEventVisual(
    FeedEventColor Color,
    string? Title,
    IReadOnlyList<string> Lines,
    string? Description,
    string? ThumbnailUrl,
    string? FooterText);

public sealed record FeedMapEventViewData(
    long SetId,
    string BeatmapsetUrl,
    string BeatmapsetTitle,
    string MapperName,
    long? MapperUserId,
    string ModeTags,
    string? Message,
    string? History);

public sealed record FeedGroupEventViewData(
    long UserId,
    string UserName,
    long GroupId,
    string GroupName,
    string? Playmodes,
    string UserUrl,
    string GroupUrl);

public sealed record FeedEventViewEntry(
    long EventId,
    FeedType FeedType,
    FeedEventType EventType,
    DateTimeOffset? CreatedAt,
    string Emoji,
    string DisplayName,
    string PrimaryUrl,
    FeedEventActor? Actor,
    FeedEventVisual Visual,
    FeedMapEventViewData? Map,
    FeedGroupEventViewData? Group);
