using MappingFeed.Common.Services.Osu;

namespace MappingFeed.Data.Services.Backfill;

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
