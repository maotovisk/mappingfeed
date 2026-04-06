using MappingFeed.Feed;

namespace MappingFeed.Data.Entities;

public sealed class BeatmapsetEvent
{
    public long SetId { get; set; }

    public long? TriggeredBy { get; set; }

    public DateTimeOffset? CreatedAt { get; set; }

    public FeedEventType EventType { get; set; }

    public string? Message { get; set; }

    public long? DiscussionId { get; set; }

    public long? PostId { get; set; }

    public long? MapperUserId { get; set; }

    public string? Rulesets { get; set; }

    public string RawEvent { get; set; } = "{}";

    public long EventId { get; set; }
}
