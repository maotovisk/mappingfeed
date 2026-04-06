using MappingFeed.Feed;

namespace MappingFeed.Data.Entities;

public sealed class GroupEvent
{
    public long UserId { get; set; }

    public string? UserName { get; set; }

    public DateTimeOffset? CreatedAt { get; set; }

    public FeedEventType EventType { get; set; }

    public long GroupId { get; set; }

    public string? GroupName { get; set; }

    public string? Playmodes { get; set; }

    public string RawEvent { get; set; } = "{}";

    public long EventId { get; set; }
}
