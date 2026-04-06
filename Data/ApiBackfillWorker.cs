using MappingFeed.Config;
using MappingFeed.Osu;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;

namespace MappingFeed.Data;

public sealed class ApiBackfillWorker(
    IDbContextFactory<MappingFeedDbContext> dbContextFactory,
    OsuApiClient osuApiClient,
    IOptions<FeedOptions> options,
    ILogger<ApiBackfillWorker> logger) : BackgroundService
{
    private readonly FeedOptions _options = options.Value;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (!_options.EnableApiBackfillWorker)
        {
            logger.LogInformation("API backfill worker is disabled.");
            return;
        }

        var startupDelaySeconds = Math.Max(0, _options.ApiBackfillStartupDelaySeconds);
        if (startupDelaySeconds > 0)
            await Task.Delay(TimeSpan.FromSeconds(startupDelaySeconds), stoppingToken);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await using var db = await dbContextFactory.CreateDbContextAsync(stoppingToken);

                var throttleDelay = TimeSpan.FromMilliseconds(Math.Max(0, _options.ApiBackfillThrottleMilliseconds));
                var batchSize = Math.Clamp(_options.ApiBackfillBatchSize, 1, 512);

                logger.LogInformation(
                    "Running API historical backfill with throttle {ThrottleMs}ms and batch size {BatchSize}.",
                    throttleDelay.TotalMilliseconds,
                    batchSize);

                await DatabaseSchemaUpdater.RunApiBackfillAsync(
                    db,
                    osuApiClient,
                    throttleDelay,
                    batchSize,
                    stoppingToken);

                logger.LogInformation("Completed API historical backfill cycle.");
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception exception)
            {
                logger.LogError(exception, "API historical backfill cycle failed.");
            }

            var repeatIntervalMinutes = Math.Max(0, _options.ApiBackfillRepeatIntervalMinutes);
            if (repeatIntervalMinutes == 0)
                break;

            await Task.Delay(TimeSpan.FromMinutes(repeatIntervalMinutes), stoppingToken);
        }
    }
}
