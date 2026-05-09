using MappingFeed.Data;

namespace MappingFeed.Services.Backfill;

public interface IApiBackfillService
{
    Task RunAsync(
        MappingFeedDbContext db,
        TimeSpan apiThrottleDelay,
        int apiBatchSize,
        CancellationToken cancellationToken = default);
}
