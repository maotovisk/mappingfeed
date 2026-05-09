using MappingFeed.Data;

namespace MappingFeed.Services.Backfill;

public sealed class ApiBackfillService(IOsuApiService osuApiService) : IApiBackfillService
{
    public Task RunAsync(
        MappingFeedDbContext db,
        TimeSpan apiThrottleDelay,
        int apiBatchSize,
        CancellationToken cancellationToken = default)
    {
        return DatabaseSchemaUpdater.RunApiBackfillAsync(
            db,
            osuApiService,
            apiThrottleDelay,
            apiBatchSize,
            cancellationToken);
    }
}
