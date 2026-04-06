using System.Collections.Concurrent;
using MappingFeed.Data;
using MappingFeed.Data.Entities;
using MappingFeed.Feed;
using Microsoft.EntityFrameworkCore;
using NetCord;
using NetCord.Rest;
using NetCord.Services.ComponentInteractions;

namespace MappingFeed.Commands;

public sealed class FeedSetupSessionStore
{
    private static readonly TimeSpan SessionTtl = TimeSpan.FromMinutes(30);
    private readonly ConcurrentDictionary<ulong, FeedSetupSession> _sessions = new();

    internal FeedSetupSession StartOrReset(ulong userId, ulong? guildId, long channelId)
    {
        CleanupExpired();

        var session = new FeedSetupSession(userId, guildId, channelId);
        _sessions[userId] = session;
        return session;
    }

    internal bool TryGet(ulong userId, out FeedSetupSession session)
    {
        if (_sessions.TryGetValue(userId, out var existing) && !IsExpired(existing))
        {
            session = existing;
            return true;
        }

        _sessions.TryRemove(userId, out _);
        session = null!;
        return false;
    }

    internal void Clear(ulong userId)
    {
        _sessions.TryRemove(userId, out _);
    }

    private void CleanupExpired()
    {
        foreach (var pair in _sessions)
        {
            if (IsExpired(pair.Value))
                _sessions.TryRemove(pair.Key, out _);
        }
    }

    private static bool IsExpired(FeedSetupSession session)
    {
        return DateTimeOffset.UtcNow - session.UpdatedAt > SessionTtl;
    }
}

internal sealed class FeedSetupSession(ulong userId, ulong? guildId, long channelId)
{
    public ulong UserId { get; } = userId;

    public ulong? GuildId { get; } = guildId;

    public long ChannelId { get; } = channelId;

    public FeedType FeedType { get; set; } = FeedType.Map;

    public HashSet<string> Rulesets { get; } = new(StringComparer.OrdinalIgnoreCase);

    public HashSet<string> EventTypes { get; } = new(StringComparer.OrdinalIgnoreCase);

    public HashSet<string> GroupIds { get; } = new(StringComparer.OrdinalIgnoreCase);

    public DateTimeOffset UpdatedAt { get; private set; } = DateTimeOffset.UtcNow;

    public void Touch()
    {
        UpdatedAt = DateTimeOffset.UtcNow;
    }
}

internal static class FeedSetupUi
{
    public const string TypeMenuCustomId = "setup_feed_type";
    public const string RulesetMenuCustomId = "setup_feed_rulesets";
    public const string EventTypeMenuCustomId = "setup_feed_event_types";
    public const string GroupMenuCustomId = "setup_feed_groups";
    public const string SaveButtonCustomId = "setup_feed_save";
    public const string CancelButtonCustomId = "setup_feed_cancel";

    private static readonly IReadOnlyList<string> RulesetValues = ["osu", "taiko", "catch", "mania"];

    private static readonly IReadOnlyList<string> EventTypeValues =
    [
        "nominate",
        "nomination_reset",
        "qualify",
        "disqualify",
        "rank",
        "unrank",
    ];

    private static readonly IReadOnlyList<GroupChoice> GroupChoices =
    [
        new("4", "GMT", "Global Moderation Team"),
        new("7", "NAT", "Nomination Assessment Team"),
        new("11", "DEV", "Developers"),
        new("16", "ALM", "osu! Alumni"),
        new("22", "SPT", "Technical Support Team"),
        new("28", "BN", "Beatmap Nominators"),
        new("29", "BOT", "Chat Bots"),
        new("31", "LVD", "Project Loved"),
        new("32", "BN", "Beatmap Nominators (Probationary)"),
        new("33", "PPY", "ppy"),
        new("35", "FA", "Featured Artist"),
        new("48", "BSC", "Beatmap Spotlight Curators"),
        new("50", "TC", "Tournament Committee"),
    ];

    private static readonly HashSet<string> RulesetValueSet = new(RulesetValues, StringComparer.OrdinalIgnoreCase);
    private static readonly HashSet<string> EventTypeValueSet = new(EventTypeValues, StringComparer.OrdinalIgnoreCase);
    private static readonly HashSet<string> GroupValueSet = new(GroupChoices.Select(x => x.Id), StringComparer.OrdinalIgnoreCase);

    public static bool IsValidRuleset(string value) => RulesetValueSet.Contains(value);

    public static bool IsValidEventType(string value) => EventTypeValueSet.Contains(value);

    public static bool IsValidGroupId(string value) => GroupValueSet.Contains(value);

    public static IEnumerable<string> OrderRulesets(IEnumerable<string> selected)
    {
        var selectedSet = new HashSet<string>(selected, StringComparer.OrdinalIgnoreCase);
        return RulesetValues.Where(selectedSet.Contains);
    }

    public static IEnumerable<string> OrderEventTypes(IEnumerable<string> selected)
    {
        var selectedSet = new HashSet<string>(selected, StringComparer.OrdinalIgnoreCase);
        return EventTypeValues.Where(selectedSet.Contains);
    }

    public static IEnumerable<string> OrderGroupIds(IEnumerable<string> selected)
    {
        return selected
            .Where(IsValidGroupId)
            .Select(x => long.TryParse(x, out var value) ? value : long.MaxValue)
            .Where(x => x != long.MaxValue)
            .Distinct()
            .OrderBy(x => x)
            .Select(x => x.ToString());
    }

    public static InteractionMessageProperties BuildMessage(FeedSetupSession session, string? notice = null)
    {
        return new InteractionMessageProperties()
            .WithContent(BuildContent(session, notice))
            .WithComponents(BuildComponents(session));
    }

    public static string BuildContent(FeedSetupSession session, string? notice = null)
    {
        var selectedType = session.FeedType.ToCommandValue();
        var rulesets = FormatOrderedSelection(OrderRulesets(session.Rulesets));
        var eventTypes = FormatOrderedSelection(OrderEventTypes(session.EventTypes));
        var groups = FormatOrderedSelection(OrderGroupIds(session.GroupIds));

        var lines = new List<string>();
        if (!string.IsNullOrWhiteSpace(notice))
            lines.Add(notice);

        lines.Add("Feed setup:");
        lines.Add($"type: `{selectedType}`");
        if (session.FeedType == FeedType.Map)
        {
            lines.Add($"rulesets: `{rulesets}`");
            lines.Add($"event types: `{eventTypes}`");
        }
        else
        {
            lines.Add($"group ids: `{groups}`");
        }

        lines.Add("Use the selectors below and press Save.");
        return string.Join('\n', lines);
    }

    public static IEnumerable<IMessageComponentProperties> BuildComponents(FeedSetupSession session)
    {
        var rulesetsDisabled = session.FeedType == FeedType.Group;
        var eventTypesDisabled = session.FeedType == FeedType.Group;
        var groupsDisabled = session.FeedType == FeedType.Map;

        return
        [
            BuildTypeMenu(session),
            BuildRulesetMenu(session, rulesetsDisabled),
            BuildEventTypeMenu(session, eventTypesDisabled),
            BuildGroupMenu(session, groupsDisabled),
            BuildActionButtons(),
        ];
    }

    private static StringMenuProperties BuildTypeMenu(FeedSetupSession session)
    {
        return new StringMenuProperties(
                TypeMenuCustomId,
                [
                    new StringMenuSelectOptionProperties("map", "map")
                        .WithDescription("Beatmap ranking event feed")
                        .WithDefault(session.FeedType == FeedType.Map),
                    new StringMenuSelectOptionProperties("group", "group")
                        .WithDescription("User group membership feed")
                        .WithDefault(session.FeedType == FeedType.Group),
                ])
            .WithPlaceholder("Feed type")
            .WithMinValues(1)
            .WithMaxValues(1);
    }

    private static StringMenuProperties BuildRulesetMenu(FeedSetupSession session, bool disabled)
    {
        var selectedSet = new HashSet<string>(session.Rulesets, StringComparer.OrdinalIgnoreCase);

        return new StringMenuProperties(
                RulesetMenuCustomId,
                RulesetValues.Select(value =>
                    new StringMenuSelectOptionProperties(value, value)
                        .WithDefault(selectedSet.Contains(value))))
            .WithPlaceholder("Rulesets (map feed only)")
            .WithMinValues(0)
            .WithMaxValues(RulesetValues.Count)
            .WithDisabled(disabled);
    }

    private static StringMenuProperties BuildEventTypeMenu(FeedSetupSession session, bool disabled)
    {
        var selectedSet = new HashSet<string>(session.EventTypes, StringComparer.OrdinalIgnoreCase);

        return new StringMenuProperties(
                EventTypeMenuCustomId,
                EventTypeValues.Select(value =>
                    new StringMenuSelectOptionProperties(value, value)
                        .WithDefault(selectedSet.Contains(value))))
            .WithPlaceholder("Event types (map feed only)")
            .WithMinValues(0)
            .WithMaxValues(EventTypeValues.Count)
            .WithDisabled(disabled);
    }

    private static StringMenuProperties BuildGroupMenu(FeedSetupSession session, bool disabled)
    {
        var selectedSet = new HashSet<string>(session.GroupIds, StringComparer.OrdinalIgnoreCase);

        return new StringMenuProperties(
                GroupMenuCustomId,
                GroupChoices.Select(choice =>
                    new StringMenuSelectOptionProperties($"{choice.Id} ({choice.Acronym})", choice.Id)
                        .WithDescription(choice.Name)
                        .WithDefault(selectedSet.Contains(choice.Id))))
            .WithPlaceholder("Group IDs (group feed only)")
            .WithMinValues(0)
            .WithMaxValues(GroupChoices.Count)
            .WithDisabled(disabled);
    }

    private static ActionRowProperties BuildActionButtons()
    {
        return new ActionRowProperties(
        [
            new ButtonProperties(SaveButtonCustomId, "Save", ButtonStyle.Success),
            new ButtonProperties(CancelButtonCustomId, "Cancel", ButtonStyle.Secondary),
        ]);
    }

    private static string FormatOrderedSelection(IEnumerable<string> orderedValues)
    {
        var values = orderedValues.ToList();
        return values.Count == 0 ? "all" : string.Join(",", values);
    }

    private readonly record struct GroupChoice(string Id, string Acronym, string Name);
}

public sealed class FeedSetupComponentModule(
    IDbContextFactory<MappingFeedDbContext> dbContextFactory,
    FeedSetupSessionStore sessionStore)
    : ComponentInteractionModule<ComponentInteractionContext>
{
    [ComponentInteraction(FeedSetupUi.TypeMenuCustomId)]
    public async Task SelectFeedTypeAsync()
    {
        if (!TryGetActiveSession(out var session, out var missingMessage))
        {
            await RespondWithSessionMissingAsync(missingMessage);
            return;
        }

        if (!Context.TryGetStringMenuInteraction(out var interaction))
        {
            await RespondWithSessionMissingAsync("Invalid interaction for feed type selection.");
            return;
        }

        var selected = interaction.Data.SelectedValues.FirstOrDefault();
        if (!FeedEnumExtensions.TryParseFeedType(selected, out var feedType))
        {
            await UpdateSetupMessageAsync(session, "Invalid feed type selection.");
            return;
        }

        session.FeedType = feedType;
        session.Touch();
        await UpdateSetupMessageAsync(session, $"Selected feed type `{feedType.ToCommandValue()}`.");
    }

    [ComponentInteraction(FeedSetupUi.RulesetMenuCustomId)]
    public async Task SelectRulesetsAsync()
    {
        if (!TryGetActiveSession(out var session, out var missingMessage))
        {
            await RespondWithSessionMissingAsync(missingMessage);
            return;
        }

        if (!Context.TryGetStringMenuInteraction(out var interaction))
        {
            await RespondWithSessionMissingAsync("Invalid interaction for ruleset selection.");
            return;
        }

        session.Rulesets.Clear();
        foreach (var selected in interaction.Data.SelectedValues.Where(FeedSetupUi.IsValidRuleset))
            session.Rulesets.Add(selected);

        session.Touch();
        await UpdateSetupMessageAsync(session, "Updated rulesets.");
    }

    [ComponentInteraction(FeedSetupUi.EventTypeMenuCustomId)]
    public async Task SelectEventTypesAsync()
    {
        if (!TryGetActiveSession(out var session, out var missingMessage))
        {
            await RespondWithSessionMissingAsync(missingMessage);
            return;
        }

        if (!Context.TryGetStringMenuInteraction(out var interaction))
        {
            await RespondWithSessionMissingAsync("Invalid interaction for event type selection.");
            return;
        }

        session.EventTypes.Clear();
        foreach (var selected in interaction.Data.SelectedValues.Where(FeedSetupUi.IsValidEventType))
            session.EventTypes.Add(selected);

        session.Touch();
        await UpdateSetupMessageAsync(session, "Updated event types.");
    }

    [ComponentInteraction(FeedSetupUi.GroupMenuCustomId)]
    public async Task SelectGroupIdsAsync()
    {
        if (!TryGetActiveSession(out var session, out var missingMessage))
        {
            await RespondWithSessionMissingAsync(missingMessage);
            return;
        }

        if (!Context.TryGetStringMenuInteraction(out var interaction))
        {
            await RespondWithSessionMissingAsync("Invalid interaction for group selection.");
            return;
        }

        session.GroupIds.Clear();
        foreach (var selected in interaction.Data.SelectedValues.Where(FeedSetupUi.IsValidGroupId))
            session.GroupIds.Add(selected);

        session.Touch();
        await UpdateSetupMessageAsync(session, "Updated group ids.");
    }

    [ComponentInteraction(FeedSetupUi.SaveButtonCustomId)]
    public async Task SaveSetupAsync()
    {
        if (!TryGetActiveSession(out var session, out var missingMessage))
        {
            await RespondWithSessionMissingAsync(missingMessage);
            return;
        }

        if (!TryBuildSubscriptionFilters(
                session,
                out var feedType,
                out var rulesets,
                out var eventTypes,
                out var groupIds,
                out var parseError))
        {
            await UpdateSetupMessageAsync(session, parseError ?? "Invalid setup input.");
            return;
        }

        var result = await FeedSubscriptionOperations.UpsertSubscriptionAsync(
            dbContextFactory,
            session.ChannelId,
            feedType,
            rulesets,
            eventTypes,
            groupIds);

        sessionStore.Clear(session.UserId);
        await RespondAsync(InteractionCallback.ModifyMessage(options =>
            options.WithContent(result).WithComponents(Array.Empty<IMessageComponentProperties>())));
    }

    [ComponentInteraction(FeedSetupUi.CancelButtonCustomId)]
    public async Task CancelSetupAsync()
    {
        if (!TryGetActiveSession(out var session, out _))
        {
            await RespondWithSessionMissingAsync("Setup session expired. Run `/setup-feed` again.");
            return;
        }

        sessionStore.Clear(session.UserId);
        await RespondAsync(InteractionCallback.ModifyMessage(options =>
            options.WithContent("Setup cancelled. Run `/setup-feed` to start again.")
                .WithComponents(Array.Empty<IMessageComponentProperties>())));
    }

    private bool TryGetActiveSession(out FeedSetupSession session, out string message)
    {
        if (sessionStore.TryGet(Context.GetUserId(), out session))
        {
            message = string.Empty;
            return true;
        }

        message = "Setup session expired. Run `/setup-feed` again.";
        return false;
    }

    private async Task UpdateSetupMessageAsync(FeedSetupSession session, string notice)
    {
        await RespondAsync(InteractionCallback.ModifyMessage(options =>
            options.WithContent(FeedSetupUi.BuildContent(session, notice))
                .WithComponents(FeedSetupUi.BuildComponents(session))));
    }

    private async Task RespondWithSessionMissingAsync(string message)
    {
        await RespondAsync(InteractionCallback.ModifyMessage(options =>
            options.WithContent(message)
                .WithComponents(Array.Empty<IMessageComponentProperties>())));
    }

    private static bool TryBuildSubscriptionFilters(
        FeedSetupSession session,
        out FeedType feedType,
        out HashSet<Ruleset>? rulesets,
        out HashSet<FeedEventType>? eventTypes,
        out HashSet<long>? groupIds,
        out string? error)
    {
        feedType = session.FeedType;
        rulesets = null;
        eventTypes = null;
        groupIds = null;
        error = null;

        if (feedType == FeedType.Map)
        {
            var parsedRulesets = new HashSet<Ruleset>();
            foreach (var selectedRuleset in FeedSetupUi.OrderRulesets(session.Rulesets))
            {
                if (!FeedEnumExtensions.TryParseRuleset(selectedRuleset, out var parsedRuleset))
                {
                    error = $"Invalid ruleset `{selectedRuleset}`.";
                    return false;
                }

                parsedRulesets.Add(parsedRuleset);
            }

            if (parsedRulesets.Count > 0)
                rulesets = parsedRulesets;

            var parsedEventTypes = new HashSet<FeedEventType>();
            foreach (var selectedEventType in FeedSetupUi.OrderEventTypes(session.EventTypes))
            {
                if (!FeedEnumExtensions.TryParseMapEventType(selectedEventType, out var parsedEventType))
                {
                    error = $"Invalid event type `{selectedEventType}`.";
                    return false;
                }

                parsedEventTypes.Add(parsedEventType);
            }

            if (parsedEventTypes.Count > 0)
                eventTypes = parsedEventTypes;
        }
        else
        {
            var parsedGroupIds = new HashSet<long>();
            foreach (var selectedGroupId in FeedSetupUi.OrderGroupIds(session.GroupIds))
            {
                if (!long.TryParse(selectedGroupId, out var parsedGroupId) || parsedGroupId <= 0)
                {
                    error = $"Invalid group id `{selectedGroupId}`.";
                    return false;
                }

                parsedGroupIds.Add(parsedGroupId);
            }

            if (parsedGroupIds.Count > 0)
                groupIds = parsedGroupIds;
        }

        return true;
    }
}

internal static class FeedSubscriptionOperations
{
    public static async Task<string> UpsertSubscriptionAsync(
        IDbContextFactory<MappingFeedDbContext> dbContextFactory,
        long channelId,
        FeedType feedType,
        HashSet<Ruleset>? rulesets,
        HashSet<FeedEventType>? eventTypes,
        HashSet<long>? groupIds,
        CancellationToken cancellationToken = default)
    {
        var serializedRulesets = FeedEnumExtensions.SerializeRulesets(rulesets);
        var serializedEventTypes = FeedEnumExtensions.SerializeEventTypes(eventTypes);
        var serializedGroupIds = FeedEnumExtensions.SerializeGroupIds(groupIds);

        await using var db = await dbContextFactory.CreateDbContextAsync(cancellationToken);

        var existingSubscription = await db.SubscribedChannels
            .FirstOrDefaultAsync(
                x => x.ChannelId == channelId && x.FeedType == feedType,
                cancellationToken);

        if (existingSubscription is not null)
        {
            var existingSerializedGroupIds = FeedEnumExtensions.SerializeGroupIds(
                FeedEnumExtensions.DeserializeGroupIds(existingSubscription.GroupId));

            if (string.Equals(existingSubscription.Rulesets, serializedRulesets, StringComparison.Ordinal) &&
                string.Equals(existingSubscription.EventTypes, serializedEventTypes, StringComparison.Ordinal) &&
                string.Equals(existingSerializedGroupIds, serializedGroupIds, StringComparison.Ordinal))
                return $"This channel is already subscribed to `{feedType.ToCommandValue()}` ({BuildFilterSummary(feedType, existingSubscription.Rulesets, existingSubscription.EventTypes, existingSerializedGroupIds)}).";

            existingSubscription.Rulesets = serializedRulesets;
            existingSubscription.EventTypes = serializedEventTypes;
            existingSubscription.GroupId = serializedGroupIds;
            await db.SaveChangesAsync(cancellationToken);
            return $"Updated `{feedType.ToCommandValue()}` subscription ({BuildFilterSummary(feedType, serializedRulesets, serializedEventTypes, serializedGroupIds)}).";
        }

        db.SubscribedChannels.Add(new SubscribedChannel
        {
            ChannelId = channelId,
            FeedType = feedType,
            LastEventId = 0,
            Rulesets = serializedRulesets,
            EventTypes = serializedEventTypes,
            GroupId = serializedGroupIds,
        });

        await db.SaveChangesAsync(cancellationToken);
        return $"Subscribed this channel to `{feedType.ToCommandValue()}` ({BuildFilterSummary(feedType, serializedRulesets, serializedEventTypes, serializedGroupIds)}).";
    }

    public static string BuildFilterSummary(
        FeedType feedType,
        string? serializedRulesets,
        string? serializedEventTypes,
        string? serializedGroupIds)
    {
        return feedType switch
        {
            FeedType.Map =>
                $"rulesets: {FeedEnumExtensions.FormatRulesetsForDisplay(serializedRulesets)}, event types: {FeedEnumExtensions.FormatEventTypesForDisplay(serializedEventTypes)}",
            FeedType.Group =>
                $"group ids: {FeedEnumExtensions.FormatGroupIdsForDisplay(serializedGroupIds)}",
            _ => "default",
        };
    }
}
