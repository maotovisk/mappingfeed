using MappingFeed.Common.Repositories.Beatmap;
using MappingFeed.Common.Repositories.Group;
using MappingFeed.Common.Repositories.SubscribedFeed;
using MappingFeed.Data.Repositories.Beatmap;
using MappingFeed.Data.Repositories.Group;
using MappingFeed.Data.Repositories.SubscribedFeed;
using MappingFeed.Data.Services.Backfill;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;

namespace MappingFeed.Data.DependencyInjection;

public static class MappingFeedDataServiceCollectionExtensions
{
    public static IServiceCollection AddMappingFeedData(this IServiceCollection services, string databasePath)
    {
        services.AddDbContextFactory<MappingFeedDbContext>(options =>
        {
            options.UseSqlite($"Data Source={databasePath}");
        });

        services.AddSingleton<IBeatmapEventRepository, BeatmapEventRepository>();
        services.AddSingleton<IGroupEventRepository, GroupEventRepository>();
        services.AddSingleton<ISubscribedFeedRepository, SubscribedFeedRepository>();
        services.AddSingleton<IApiBackfillService, ApiBackfillService>();

        return services;
    }
}
