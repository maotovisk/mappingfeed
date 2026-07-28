using MappingFeed.Common.Config;
using MappingFeed.Discord.Dispatchers.Events;
using Microsoft.Extensions.Options;

namespace MappingFeed.Web.Workers;

public sealed class FeedingDispatcherWorker(
    FeedEventsDispatcher eventsDispatcher,
    IOptions<FeedOptions> options,
    ILogger<FeedingDispatcherWorker> logger) : BackgroundService
{
    private const int MinDispatchIntervalSeconds = 180;

    private readonly FeedOptions _options = options.Value;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await eventsDispatcher.DispatchAsync(stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception exception)
            {
                logger.LogError(exception, "Failed while sending feed events.");
            }

            await Task.Delay(TimeSpan.FromSeconds(GetDispatchIntervalSeconds()), stoppingToken);
        }
    }

    private int GetDispatchIntervalSeconds()
    {
        return Math.Max(_options.DispatchIntervalSeconds, MinDispatchIntervalSeconds);
    }
}
