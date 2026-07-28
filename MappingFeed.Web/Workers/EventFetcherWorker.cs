using MappingFeed.Common.Config;
using MappingFeed.Scraper.Fetchers.Events;
using Microsoft.Extensions.Options;

namespace MappingFeed.Web.Workers;

public sealed class EventFetcherWorker(
    BeatmapEventsFetcher beatmapEventsFetcher,
    GroupEventsFetcher groupEventsFetcher,
    IOptions<FeedOptions> options,
    ILogger<EventFetcherWorker> logger) : BackgroundService
{
    private readonly FeedOptions _options = options.Value;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await beatmapEventsFetcher.FetchAsync(stoppingToken);
                await groupEventsFetcher.FetchAsync(stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception exception)
            {
                logger.LogError(exception, "Failed while fetching osu! feed events.");
            }

            await Task.Delay(TimeSpan.FromSeconds(_options.PollIntervalSeconds), stoppingToken);
        }
    }
}
