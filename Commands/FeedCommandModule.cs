using NetCord;
using NetCord.Rest;
using NetCord.Services.ApplicationCommands;

namespace MappingFeed.Commands;

public sealed class FeedCommandModule(
    ISubscribedFeedService subscribedFeedService,
    FeedSetupSessionStore setupSessionStore)
    : ApplicationCommandModule<ApplicationCommandContext>
{
    [SlashCommand("setup-feed", "Interactive setup form for the current channel feed subscription.")]
    public InteractionMessageProperties SetupFeedAsync()
    {
        if (!Context.IsGuildInteraction())
            return new InteractionMessageProperties()
                .WithContent("This command only works in server channels.")
                .WithFlags(MessageFlags.Ephemeral);

        var channelId = Context.GetChannelId();
        var session = setupSessionStore.StartOrReset(
            Context.GetUserId(),
            Context.GetGuildId(),
            channelId);

        return FeedSetupUi.BuildMessage(session, "Pick the options below, then press Save.")
            .WithFlags(MessageFlags.Ephemeral);
    }

    [SlashCommand("unsubscribe-feed", "Unsubscribe the current channel from a feed type (supports optional ruleset argument syntax).")]
    public async Task<string> UnsubscribeFeedAsync(
        [SlashCommandParameter(
            Description = "map/group (ruleset:... is accepted but ignored for matching)",
            AutocompleteProviderType = typeof(FeedTypeAutocompleteProvider))]
        string type)
    {
        if (!FeedEnumExtensions.TryParseFeedTypeArgument(type, out var feedType))
            return "Invalid feed type. Use `map` or `group`.";

        if (!Context.IsGuildInteraction())
            return "This command only works in server channels.";

        var channelId = Context.GetChannelId();

        var deleted = await subscribedFeedService.DeleteSubscriptionAsync(channelId, feedType);
        if (!deleted)
            return $"This channel is not subscribed to `{feedType.ToCommandValue()}`.";

        return $"Unsubscribed this channel from `{feedType.ToCommandValue()}`.";
    }

    [SlashCommand("feed-status", "Show which feed types are enabled in this channel.")]
    public async Task<string> FeedStatusAsync()
    {
        if (!Context.IsGuildInteraction())
            return "This command only works in server channels.";

        var channelId = Context.GetChannelId();

        var subscriptions = await subscribedFeedService.GetChannelSubscriptionsAsync(channelId);

        if (subscriptions.Count == 0)
            return "This channel has no feed subscriptions.";

        var status = string.Join(", ", subscriptions.Select(x =>
            $"{x.FeedType.ToCommandValue()} ({SubscribedFeedService.BuildFilterSummary(x.FeedType, x.Rulesets, x.EventTypes, x.GroupId)})"));

        return $"Enabled feeds: {status}";
    }
}
