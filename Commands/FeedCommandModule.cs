using MappingFeed.Data;
using MappingFeed.Data.Entities;
using MappingFeed.Feed;
using Microsoft.EntityFrameworkCore;
using NetCord.Services.ApplicationCommands;

namespace MappingFeed.Commands;

public sealed class FeedCommandModule(IDbContextFactory<MappingFeedDbContext> dbContextFactory)
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
            Description = "Optional group id (e.g. 28)",
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
                out var parsedGroupId,
                out var parseError))
            return parseError ?? "Invalid argument.";

        if (Context.Interaction.GuildId is null)
            return "This command only works in server channels.";

        var channelId = checked((long)Context.Channel.Id);
        var serializedRulesets = FeedEnumExtensions.SerializeRulesets(rulesets);
        var serializedEventTypes = FeedEnumExtensions.SerializeEventTypes(eventTypes);

        await using var db = await dbContextFactory.CreateDbContextAsync();

        var existingSubscription = await db.SubscribedChannels
            .FirstOrDefaultAsync(x => x.ChannelId == channelId && x.FeedType == feedType);

        if (existingSubscription is not null)
        {
            if (string.Equals(existingSubscription.Rulesets, serializedRulesets, StringComparison.Ordinal) &&
                string.Equals(existingSubscription.EventTypes, serializedEventTypes, StringComparison.Ordinal) &&
                existingSubscription.GroupId == parsedGroupId)
                return $"This channel is already subscribed to `{feedType.ToCommandValue()}` ({BuildFilterSummary(feedType, existingSubscription.Rulesets, existingSubscription.EventTypes, existingSubscription.GroupId)}).";

            existingSubscription.Rulesets = serializedRulesets;
            existingSubscription.EventTypes = serializedEventTypes;
            existingSubscription.GroupId = parsedGroupId;
            await db.SaveChangesAsync();
            return $"Updated `{feedType.ToCommandValue()}` subscription ({BuildFilterSummary(feedType, serializedRulesets, serializedEventTypes, parsedGroupId)}).";
        }

        db.SubscribedChannels.Add(new SubscribedChannel
        {
            ChannelId = channelId,
            FeedType = feedType,
            LastEventId = 0,
            Rulesets = serializedRulesets,
            EventTypes = serializedEventTypes,
            GroupId = parsedGroupId,
        });

        await db.SaveChangesAsync();
        return $"Subscribed this channel to `{feedType.ToCommandValue()}` ({BuildFilterSummary(feedType, serializedRulesets, serializedEventTypes, parsedGroupId)}).";
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
            $"{x.FeedType.ToCommandValue()} ({BuildFilterSummary(x.FeedType, x.Rulesets, x.EventTypes, x.GroupId)})"));

        return $"Enabled feeds: {status}";
    }

    private static string BuildFilterSummary(
        FeedType feedType,
        string? serializedRulesets,
        string? serializedEventTypes,
        long? groupId)
    {
        return feedType switch
        {
            FeedType.Map =>
                $"rulesets: {FeedEnumExtensions.FormatRulesetsForDisplay(serializedRulesets)}, event types: {FeedEnumExtensions.FormatEventTypesForDisplay(serializedEventTypes)}",
            FeedType.Group =>
                $"group id: {(groupId is null ? "all" : groupId.Value.ToString())}",
            _ => "default",
        };
    }
}
