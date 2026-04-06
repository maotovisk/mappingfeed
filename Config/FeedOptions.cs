namespace MappingFeed.Config;

public sealed class FeedOptions
{
    public const string SectionName = "Feed";

    public int PollIntervalSeconds { get; set; } = 30;

    public int DispatchIntervalSeconds { get; set; } = 180;

    public int EventsBatchSize { get; set; } = 25;

    public int DispatchBatchSize { get; set; } = 10;

    public int ApiCacheMinutes { get; set; } = 10;

    public bool EnableApiBackfillWorker { get; set; } = true;

    public int ApiBackfillStartupDelaySeconds { get; set; } = 20;

    public int ApiBackfillRepeatIntervalMinutes { get; set; } = 0;

    public int ApiBackfillThrottleMilliseconds { get; set; } = 2500;

    public int ApiBackfillBatchSize { get; set; } = 25;
}
