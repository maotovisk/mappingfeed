using MappingFeed.Commands;
using MappingFeed.Config;
using MappingFeed.Data;
using MappingFeed.Feed;
using MappingFeed.Osu;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;
using NetCord.Hosting.Gateway;
using NetCord.Hosting.Services.ApplicationCommands;
using NetCord.Gateway;
using NetCord.Services.ApplicationCommands;
using NetCord.Hosting.Services.ComponentInteractions;

var builder = Host.CreateApplicationBuilder(args);

var environmentFileName = $"appsettings.{builder.Environment.EnvironmentName}.json";
var configurationCandidates = new[]
{
    Path.Combine(builder.Environment.ContentRootPath, environmentFileName),
    Path.Combine(AppContext.BaseDirectory, environmentFileName),
};

var selectedConfigurationPath = configurationCandidates.FirstOrDefault(File.Exists)
    ?? throw new InvalidOperationException(
        $"Configuration file '{environmentFileName}' was not found. " +
        $"Checked: {string.Join(", ", configurationCandidates)}");

builder.Configuration.Sources.Clear();
builder.Configuration
    .AddJsonFile(selectedConfigurationPath, optional: false, reloadOnChange: true)
    .AddEnvironmentVariables();

var configuredDiscordToken = builder.Configuration[$"{DiscordOptions.SectionName}:Token"];
if (string.IsNullOrWhiteSpace(configuredDiscordToken))
{
    throw new InvalidOperationException(
        $"Discord token is empty for environment '{builder.Environment.EnvironmentName}'. " +
        $"Set '{DiscordOptions.SectionName}:Token' in '{selectedConfigurationPath}' " +
        $"or via environment variable '{DiscordOptions.SectionName}__Token'.");
}

builder.Services.Configure<DiscordOptions>(builder.Configuration.GetSection(DiscordOptions.SectionName));
builder.Services.Configure<OsuOptions>(builder.Configuration.GetSection(OsuOptions.SectionName));
builder.Services.Configure<FeedOptions>(builder.Configuration.GetSection(FeedOptions.SectionName));

builder.Services.AddDiscordGateway((options, serviceProvider) =>
{
    var discordOptions = serviceProvider.GetRequiredService<IOptions<DiscordOptions>>().Value;
    options.Token = discordOptions.Token;
    options.Intents = GatewayIntents.Guilds;
});

builder.Services.AddApplicationCommands();
builder.Services.AddComponentInteractions();

var localAppDataPath = Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData);
if (string.IsNullOrWhiteSpace(localAppDataPath))
    throw new InvalidOperationException("Could not resolve local application data path.");

var databaseDirectory = Path.Combine(localAppDataPath, "mappingfeed");
Directory.CreateDirectory(databaseDirectory);
var databasePath = Path.Combine(databaseDirectory, "db.sqlite");

builder.Services.AddDbContextFactory<MappingFeedDbContext>(options =>
{
    options.UseSqlite($"Data Source={databasePath}");
});

builder.Services.AddMemoryCache();
builder.Services.AddSingleton<FeedEmbedFactory>();
builder.Services.AddSingleton<FeedSetupSessionStore>();
builder.Services.AddSingleton<FeedTypeAutocompleteProvider>();
builder.Services.AddSingleton<SubscribeGroupIdAutocompleteProvider>();
builder.Services.AddSingleton<SubscribeEventTypeAutocompleteProvider>();
builder.Services.AddSingleton<SubscribeRulesetAutocompleteProvider>();

builder.Services.AddHttpClient<OsuAuthClient>((serviceProvider, client) =>
{
    var osuOptions = serviceProvider.GetRequiredService<IOptions<OsuOptions>>().Value;
    client.BaseAddress = new Uri(osuOptions.BaseUrl);
});

builder.Services.AddHttpClient<OsuApiClient>((serviceProvider, client) =>
{
    var osuOptions = serviceProvider.GetRequiredService<IOptions<OsuOptions>>().Value;
    client.BaseAddress = new Uri(osuOptions.BaseUrl);
});

builder.Services.AddHostedService<FeedFetchingWorker>();
builder.Services.AddHostedService<FeedSendingWorker>();

var host = builder.Build();

await using (var scope = host.Services.CreateAsyncScope())
{
    var dbContextFactory = scope.ServiceProvider.GetRequiredService<IDbContextFactory<MappingFeedDbContext>>();
    await using var db = await dbContextFactory.CreateDbContextAsync();
    await DatabaseSchemaUpdater.EnsureUpdatedAsync(db);
}

host.Services
    .GetRequiredService<IApplicationCommandService>()
    .AddModule<FeedCommandModule>();
host.AddComponentInteractionModule<FeedSetupComponentModule>();

await host.RunAsync();
