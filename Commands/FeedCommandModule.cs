using MappingFeed.Data;
using MappingFeed.Feed;
using Microsoft.EntityFrameworkCore;
using NetCord;
using NetCord.Rest;
using NetCord.Services.ApplicationCommands;

namespace MappingFeed.Commands;

public sealed class FeedCommandModule(
    IDbContextFactory<MappingFeedDbContext> dbContextFactory,
    FeedSetupSessionStore setupSessionStore)
    : ApplicationCommandModule<ApplicationCommandContext>
{
    [SlashCommand("subscribe-feed", "Subscribe the current channel to a feed type.")]
    public async Task<string> SubscribeFeedAsync(
        [SlashCommandParameter(
            Description = "map/group",
            AutocompleteProviderType = typeof(FeedTypeAutocompleteProvider))]
        string type,
        [SlashCommandParameter(
            Description = "Optional map rulesets (e.g. osu or osu,mania)",
            AutocompleteProviderType = typeof(SubscribeRulesetAutocompleteProvider))]
        string? ruleset = null,
        [SlashCommandParameter(
            Description = "Optional map event types (e.g. rank or rank,qualify)",
            AutocompleteProviderType = typeof(SubscribeEventTypeAutocompleteProvider))]
        string? eventType = null,
        [SlashCommandParameter(
            Description = "Optional group ids (e.g. 28 or 28,32)",
            AutocompleteProviderType = typeof(SubscribeGroupIdAutocompleteProvider))]
        string? groupId = null)
    {
        var subscribeParts = new List<string> { type };
        if (!string.IsNullOrWhiteSpace(ruleset))
            subscribeParts.Add($"ruleset:{ruleset}");
        if (!string.IsNullOrWhiteSpace(eventType))
            subscribeParts.Add($"event_type:{eventType}");
        if (!string.IsNullOrWhiteSpace(groupId))
            subscribeParts.Add($"group_id:{groupId}");

        var subscribeInput = string.Join(' ', subscribeParts);

        if (!FeedEnumExtensions.TryParseSubscribeArgument(
                subscribeInput,
                out var feedType,
                out var rulesets,
                out var eventTypes,
                out var parsedGroupIds,
                out var parseError))
            return parseError ?? "Invalid argument.";

        if (Context.Interaction.GuildId is null)
            return "This command only works in server channels.";

        var channelId = checked((long)Context.Channel.Id);
        return await FeedSubscriptionOperations.UpsertSubscriptionAsync(
            dbContextFactory,
            channelId,
            feedType,
            rulesets,
            eventTypes,
            parsedGroupIds);
    }

    [SlashCommand("setup-feed", "Interactive setup form for the current channel feed subscription.")]
    public InteractionMessageProperties SetupFeedAsync()
    {
        if (Context.Interaction.GuildId is null)
            return new InteractionMessageProperties()
                .WithContent("This command only works in server channels.")
                .WithFlags(MessageFlags.Ephemeral);

        var channelId = checked((long)Context.Channel.Id);
        var session = setupSessionStore.StartOrReset(
            Context.User.Id,
            Context.Interaction.GuildId,
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

        if (Context.Interaction.GuildId is null)
            return "This command only works in server channels.";

        var channelId = checked((long)Context.Channel.Id);

        await using var db = await dbContextFactory.CreateDbContextAsync();

        var subscription = await db.SubscribedChannels
            .FirstOrDefaultAsync(x => x.ChannelId == channelId && x.FeedType == feedType);

        if (subscription is null)
            return $"This channel is not subscribed to `{feedType.ToCommandValue()}`.";

        db.SubscribedChannels.Remove(subscription);
        await db.SaveChangesAsync();

        return $"Unsubscribed this channel from `{feedType.ToCommandValue()}`.";
    }

    [SlashCommand("feed-status", "Show which feed types are enabled in this channel.")]
    public async Task<string> FeedStatusAsync()
    {
        if (Context.Interaction.GuildId is null)
            return "This command only works in server channels.";

        var channelId = checked((long)Context.Channel.Id);

        await using var db = await dbContextFactory.CreateDbContextAsync();

        var subscriptions = await db.SubscribedChannels
            .Where(x => x.ChannelId == channelId)
            .OrderBy(x => x.FeedType)
            .ToListAsync();

        if (subscriptions.Count == 0)
            return "This channel has no feed subscriptions.";

        var status = string.Join(", ", subscriptions.Select(x =>
            $"{x.FeedType.ToCommandValue()} ({FeedSubscriptionOperations.BuildFilterSummary(x.FeedType, x.Rulesets, x.EventTypes, x.GroupId)})"));

        return $"Enabled feeds: {status}";
    }
}
