using MappingFeed.Common.Services.Beatmap;
using MappingFeed.Common.Services.Group;
using MappingFeed.Common.Services.SubscribedFeed;
using MappingFeed.Web.Services.Beatmap;
using MappingFeed.Web.Services.Group;
using MappingFeed.Web.Services.SubscribedFeed;
using MappingFeed.Web.Workers;

namespace MappingFeed.Web.DependencyInjection;

public static class MappingFeedWebServiceCollectionExtensions
{
    public static IServiceCollection AddMappingFeedWeb(this IServiceCollection services)
    {
        services.AddSingleton<IBeatmapEventService, BeatmapEventService>();
        services.AddSingleton<IGroupEventService, GroupEventService>();
        services.AddSingleton<ISubscribedFeedService, SubscribedFeedService>();

        services.AddHostedService<EventFetcherWorker>();
        services.AddHostedService<FeedingDispatcherWorker>();
        services.AddHostedService<ApiBackfillWorker>();

        return services;
    }
}
