using MappingFeed.Discord.Commands;
using MappingFeed.Discord.Dispatchers.Events;
using MappingFeed.Discord.Events.EmbedFactories;
using Microsoft.Extensions.DependencyInjection;

namespace MappingFeed.Discord.DependencyInjection;

public static class MappingFeedDiscordServiceCollectionExtensions
{
    public static IServiceCollection AddMappingFeedDiscord(this IServiceCollection services)
    {
        services.AddSingleton<FeedEmbedFactory>();
        services.AddSingleton<FeedSetupSessionStore>();
        services.AddSingleton<FeedTypeAutocompleteProvider>();

        services.AddSingleton<FeedEventsDispatcher>();
        services.AddSingleton<BeatmapEventsDispatcher>();
        services.AddSingleton<GroupEventsDispatcher>();

        return services;
    }
}
