namespace MappingFeed.Feed;

public enum FeedType
{
    Map,
    Group,
}

public enum Ruleset
{
    Osu,
    Taiko,
    Catch,
    Mania,
}

public enum FeedEventType
{
    Nomination,
    NominationReset,
    Qualification,
    Disqualification,
    Ranked,
    Unranked,
    GroupAdd,
    GroupRemove,
    GroupMove,
}

public static class FeedEnumExtensions
{
    public static FeedType ToFeedType(this FeedEventType eventType) => eventType switch
    {
        FeedEventType.GroupAdd => FeedType.Group,
        FeedEventType.GroupRemove => FeedType.Group,
        FeedEventType.GroupMove => FeedType.Group,
        _ => FeedType.Map,
    };

    public static string ToDisplayName(this FeedEventType eventType) => eventType switch
    {
        FeedEventType.Nomination => "Nomination",
        FeedEventType.NominationReset => "Nomination Reset",
        FeedEventType.Qualification => "Qualification",
        FeedEventType.Disqualification => "Disqualification",
        FeedEventType.Ranked => "Ranked",
        FeedEventType.Unranked => "Unranked",
        FeedEventType.GroupAdd => "Added",
        FeedEventType.GroupRemove => "Removed",
        FeedEventType.GroupMove => "Group Move",
        _ => eventType.ToString(),
    };

    public static bool TryParseFeedType(string? value, out FeedType feedType)
    {
        switch (value?.Trim().ToLowerInvariant())
        {
            case "map":
                feedType = FeedType.Map;
                return true;
            case "group":
                feedType = FeedType.Group;
                return true;
            default:
                feedType = default;
                return false;
        }
    }

    public static bool TryParseFeedTypeArgument(string? value, out FeedType feedType)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            feedType = default;
            return false;
        }

        foreach (var token in value.Split(' ', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
        {
            if (TryParseFeedType(token, out feedType))
                return true;
        }

        feedType = default;
        return false;
    }

    public static string ToCommandValue(this FeedType feedType) => feedType switch
    {
        FeedType.Map => "map",
        FeedType.Group => "group",
        _ => feedType.ToString().ToLowerInvariant(),
    };

    public static string ToCommandValue(this Ruleset ruleset) => ruleset switch
    {
        Ruleset.Osu => "osu",
        Ruleset.Taiko => "taiko",
        Ruleset.Catch => "catch",
        Ruleset.Mania => "mania",
        _ => ruleset.ToString().ToLowerInvariant(),
    };

    public static string ToCommandValue(this FeedEventType eventType) => eventType switch
    {
        FeedEventType.Nomination => "nominate",
        FeedEventType.NominationReset => "nomination_reset",
        FeedEventType.Qualification => "qualify",
        FeedEventType.Disqualification => "disqualify",
        FeedEventType.Ranked => "rank",
        FeedEventType.Unranked => "unrank",
        FeedEventType.GroupAdd => "group_add",
        FeedEventType.GroupRemove => "group_remove",
        FeedEventType.GroupMove => "group_move",
        _ => eventType.ToString().ToLowerInvariant(),
    };

    public static bool TryParseRuleset(string? value, out Ruleset ruleset)
    {
        switch (value?.Trim().ToLowerInvariant())
        {
            case "osu":
                ruleset = Ruleset.Osu;
                return true;
            case "taiko":
                ruleset = Ruleset.Taiko;
                return true;
            case "catch":
            case "fruits":
                ruleset = Ruleset.Catch;
                return true;
            case "mania":
                ruleset = Ruleset.Mania;
                return true;
            default:
                ruleset = default;
                return false;
        }
    }

    public static bool TryParseMapEventType(string? value, out FeedEventType eventType)
    {
        switch (value?.Trim().ToLowerInvariant())
        {
            case "nomination":
            case "nominate":
                eventType = FeedEventType.Nomination;
                return true;
            case "nomination_reset":
            case "nominationreset":
            case "reset":
                eventType = FeedEventType.NominationReset;
                return true;
            case "qualification":
            case "qualify":
                eventType = FeedEventType.Qualification;
                return true;
            case "disqualification":
            case "disqualify":
                eventType = FeedEventType.Disqualification;
                return true;
            case "rank":
            case "ranked":
                eventType = FeedEventType.Ranked;
                return true;
            case "unrank":
            case "unranked":
                eventType = FeedEventType.Unranked;
                return true;
            default:
                eventType = default;
                return false;
        }
    }

    public static string? SerializeRulesets(IReadOnlyCollection<Ruleset>? rulesets)
    {
        if (rulesets is null || rulesets.Count == 0)
            return null;

        return string.Join("|", rulesets
            .OrderBy(x => x)
            .Select(ToCommandValue));
    }

    public static HashSet<Ruleset>? DeserializeRulesets(string? serializedRulesets)
    {
        if (string.IsNullOrWhiteSpace(serializedRulesets))
            return null;

        var rulesets = new HashSet<Ruleset>();

        foreach (var part in serializedRulesets.Split('|', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
        {
            if (TryParseRuleset(part, out var ruleset))
                rulesets.Add(ruleset);
        }

        return rulesets.Count == 0 ? null : rulesets;
    }

    public static string FormatRulesetsForDisplay(string? serializedRulesets)
    {
        var rulesets = DeserializeRulesets(serializedRulesets);
        if (rulesets is null)
            return "all";

        return string.Join(", ", rulesets
            .OrderBy(x => x)
            .Select(ToCommandValue));
    }

    public static string? SerializeEventTypes(IReadOnlyCollection<FeedEventType>? eventTypes)
    {
        if (eventTypes is null || eventTypes.Count == 0)
            return null;

        return string.Join("|", eventTypes
            .OrderBy(x => x)
            .Select(ToCommandValue));
    }

    public static HashSet<FeedEventType>? DeserializeEventTypes(string? serializedEventTypes)
    {
        if (string.IsNullOrWhiteSpace(serializedEventTypes))
            return null;

        var eventTypes = new HashSet<FeedEventType>();

        foreach (var part in serializedEventTypes.Split('|', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
        {
            if (TryParseMapEventType(part, out var eventType))
                eventTypes.Add(eventType);
        }

        return eventTypes.Count == 0 ? null : eventTypes;
    }

    public static string FormatEventTypesForDisplay(string? serializedEventTypes)
    {
        var eventTypes = DeserializeEventTypes(serializedEventTypes);
        if (eventTypes is null)
            return "all";

        return string.Join(", ", eventTypes
            .OrderBy(x => x)
            .Select(ToCommandValue));
    }

    public static string? SerializeGroupIds(IReadOnlyCollection<long>? groupIds)
    {
        if (groupIds is null || groupIds.Count == 0)
            return null;

        return string.Join("|", groupIds
            .Where(x => x > 0)
            .Distinct()
            .OrderBy(x => x));
    }

    public static HashSet<long>? DeserializeGroupIds(string? serializedGroupIds)
    {
        if (string.IsNullOrWhiteSpace(serializedGroupIds))
            return null;

        var groupIds = new HashSet<long>();
        foreach (var part in serializedGroupIds.Split([',', '|'], StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
        {
            if (long.TryParse(part, out var groupId) && groupId > 0)
                groupIds.Add(groupId);
        }

        return groupIds.Count == 0 ? null : groupIds;
    }

    public static string FormatGroupIdsForDisplay(string? serializedGroupIds)
    {
        var groupIds = DeserializeGroupIds(serializedGroupIds);
        if (groupIds is null)
            return "all";

        return string.Join(", ", groupIds
            .OrderBy(x => x));
    }
}
