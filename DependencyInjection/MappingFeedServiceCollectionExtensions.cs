using MappingFeed.Config;
using MappingFeed.Commands;
using MappingFeed.Data;
using MappingFeed.Dispatchers.Events;
using MappingFeed.Events.EmbedFactories;
using MappingFeed.Fetchers.Events;
using MappingFeed.Repositories.Beatmap;
using MappingFeed.Repositories.Group;
using MappingFeed.Repositories.SubscribedFeed;
using MappingFeed.Services.Beatmap;
using MappingFeed.Services.Backfill;
using MappingFeed.Services.Group;
using MappingFeed.Services.Osu;
using MappingFeed.Services.SubscribedFeed;
using MappingFeed.Workers;
using Microsoft.Extensions.Options;

namespace MappingFeed.DependencyInjection;

public static class MappingFeedServiceCollectionExtensions
{
    public static IServiceCollection AddMappingFeedServices(this IServiceCollection services)
    {
        services.AddSingleton<FeedEmbedFactory>();
        services.AddSingleton<FeedSetupSessionStore>();
        services.AddSingleton<FeedTypeAutocompleteProvider>();

        services.AddSingleton<IBeatmapEventRepository, BeatmapEventRepository>();
        services.AddSingleton<IGroupEventRepository, GroupEventRepository>();
        services.AddSingleton<ISubscribedFeedRepository, SubscribedFeedRepository>();

        services.AddSingleton<IBeatmapEventService, BeatmapEventService>();
        services.AddSingleton<IGroupEventService, GroupEventService>();
        services.AddSingleton<ISubscribedFeedService, SubscribedFeedService>();
        services.AddSingleton<IApiBackfillService, ApiBackfillService>();

        services.AddSingleton<BeatmapEventsFetcher>();
        services.AddSingleton<GroupEventsFetcher>();
        services.AddSingleton<FeedEventsDispatcher>();
        services.AddSingleton<BeatmapEventsDispatcher>();
        services.AddSingleton<GroupEventsDispatcher>();

        services.AddHttpClient<OsuAuthClient>(ConfigureOsuHttpClient);
        services.AddHttpClient<IOsuApiService, OsuApiService>(ConfigureOsuHttpClient);

        services.AddHostedService<EventFetcherWorker>();
        services.AddHostedService<FeedingDispatcherWorker>();
        services.AddHostedService<ApiBackfillWorker>();

        return services;
    }

    private static void ConfigureOsuHttpClient(IServiceProvider serviceProvider, HttpClient client)
    {
        var osuOptions = serviceProvider.GetRequiredService<IOptions<OsuOptions>>().Value;
        client.BaseAddress = new Uri(osuOptions.BaseUrl);
    }
}
