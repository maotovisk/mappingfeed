using MappingFeed.Feed;

namespace MappingFeed.Data.Entities;

public sealed class SubscribedChannel
{
    public long ChannelId { get; set; }

    public FeedType FeedType { get; set; }

    public long LastEventId { get; set; }

    public string? Rulesets { get; set; }

    public string? EventTypes { get; set; }

    public long? GroupId { get; set; }
}
