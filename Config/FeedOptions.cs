namespace MappingFeed.Config;

public sealed class FeedOptions
{
    public const string SectionName = "Feed";

    public int PollIntervalSeconds { get; set; } = 30;

    public int DispatchIntervalSeconds { get; set; } = 180;

    public int EventsBatchSize { get; set; } = 25;

    public int DispatchBatchSize { get; set; } = 10;

    public int ApiCacheMinutes { get; set; } = 10;
}
