using MappingFeed.Common.Config;
using MappingFeed.Common.Services.Osu;
using MappingFeed.Scraper.Fetchers.Events;
using MappingFeed.Scraper.Services.Osu;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace MappingFeed.Scraper.DependencyInjection;

public static class MappingFeedScraperServiceCollectionExtensions
{
    public static IServiceCollection AddMappingFeedScraper(this IServiceCollection services)
    {
        services.AddMemoryCache();

        services.AddHttpClient<OsuAuthClient>(ConfigureOsuHttpClient);
        services.AddHttpClient<IOsuApiService, OsuApiService>(ConfigureOsuHttpClient);

        services.AddSingleton<BeatmapEventsFetcher>();
        services.AddSingleton<GroupEventsFetcher>();

        return services;
    }

    private static void ConfigureOsuHttpClient(IServiceProvider serviceProvider, HttpClient client)
    {
        var osuOptions = serviceProvider.GetRequiredService<IOptions<OsuOptions>>().Value;
        client.BaseAddress = new Uri(osuOptions.BaseUrl);
    }
}
