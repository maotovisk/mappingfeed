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

    public static bool TryParseSubscribeArgument(
        string? value,
        out FeedType feedType,
        out HashSet<Ruleset>? rulesets,
        out HashSet<FeedEventType>? eventTypes,
        out HashSet<long>? groupIds,
        out string? error)
    {
        rulesets = null;
        eventTypes = null;
        groupIds = null;
        error = null;

        if (string.IsNullOrWhiteSpace(value))
        {
            feedType = default;
            error = "Invalid argument. Use `map` or `group`; optional `ruleset:osu,mania`, `event_type:rank,qualify`, or `group_id:28,32`.";
            return false;
        }

        var rawInput = value.Trim();

        // Preferred format: map,osu,mania
        if (rawInput.Contains(',') && !rawInput.Contains(':'))
        {
            var parts = rawInput.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
            feedType = default;
            if (parts.Length == 0 || !TryParseFeedType(parts[0], out feedType))
            {
                error = "First value must be feed type (`map` or `group`).";
                return false;
            }

            if (feedType == FeedType.Group && parts.Length > 1)
            {
                error = "Ruleset filter only applies to `map` feed subscriptions.";
                return false;
            }

            if (parts.Length == 1)
                return true;

            var parsedRulesetsCsv = new HashSet<Ruleset>();
            for (var i = 1; i < parts.Length; i++)
            {
                if (!TryParseRuleset(parts[i], out var ruleset))
                {
                    error = $"Invalid ruleset `{parts[i]}`. Use `osu`, `mania`, `taiko`, or `catch`.";
                    return false;
                }

                parsedRulesetsCsv.Add(ruleset);
            }

            rulesets = parsedRulesetsCsv.Count == 0 ? null : parsedRulesetsCsv;
            return true;
        }

        var tokens = rawInput.Split(' ', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        string? rulesetToken = null;
        string? eventTypeToken = null;
        string? groupIdToken = null;
        FeedType? parsedFeedType = null;

        foreach (var token in tokens)
        {
            if (token.StartsWith("ruleset:", StringComparison.OrdinalIgnoreCase))
            {
                rulesetToken = token["ruleset:".Length..];
                continue;
            }
            if (token.StartsWith("event_type:", StringComparison.OrdinalIgnoreCase))
            {
                eventTypeToken = token["event_type:".Length..];
                continue;
            }
            if (token.StartsWith("event_types:", StringComparison.OrdinalIgnoreCase))
            {
                eventTypeToken = token["event_types:".Length..];
                continue;
            }
            if (token.StartsWith("group_id:", StringComparison.OrdinalIgnoreCase))
            {
                groupIdToken = token["group_id:".Length..];
                continue;
            }

            if (TryParseFeedType(token, out var parsed))
            {
                parsedFeedType = parsed;
                continue;
            }

            feedType = default;
            error = $"Unknown token `{token}`. Use `map` or `group`, and optional `ruleset:osu,mania`, `event_type:rank,qualify`, `group_id:28,32`.";
            return false;
        }

        if (parsedFeedType is null)
        {
            feedType = default;
            error = "Missing feed type. Use `map` or `group`.";
            return false;
        }

        feedType = parsedFeedType.Value;

        if (!string.IsNullOrWhiteSpace(rulesetToken))
        {
            var cleaned = rulesetToken.Trim().Trim('[', ']');
            var parsedRulesetsToken = new HashSet<Ruleset>();

            foreach (var item in cleaned.Split([',', '|'], StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
            {
                if (!TryParseRuleset(item, out var ruleset))
                {
                    error = $"Invalid ruleset `{item}`. Use `osu`, `mania`, `taiko`, or `catch`.";
                    return false;
                }

                parsedRulesetsToken.Add(ruleset);
            }

            if (parsedRulesetsToken.Count == 0)
            {
                error = "Ruleset filter is empty. Use for example `ruleset:osu,mania`.";
                return false;
            }

            rulesets = parsedRulesetsToken;
        }

        if (!string.IsNullOrWhiteSpace(eventTypeToken))
        {
            var cleaned = eventTypeToken.Trim().Trim('[', ']');
            var parsedEventTypesToken = new HashSet<FeedEventType>();

            foreach (var item in cleaned.Split([',', '|'], StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
            {
                if (!TryParseMapEventType(item, out var eventTypeItem))
                {
                    error = $"Invalid event type `{item}`. Use `nominate`, `nomination_reset`, `qualify`, `disqualify`, `rank`, or `unrank`.";
                    return false;
                }

                parsedEventTypesToken.Add(eventTypeItem);
            }

            if (parsedEventTypesToken.Count == 0)
            {
                error = "Event type filter is empty. Use for example `event_type:rank,qualify`.";
                return false;
            }

            eventTypes = parsedEventTypesToken;
        }

        if (!string.IsNullOrWhiteSpace(groupIdToken))
        {
            var cleaned = groupIdToken.Trim().Trim('[', ']');
            var parsedGroupIdsToken = new HashSet<long>();

            foreach (var item in cleaned.Split([',', '|'], StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
            {
                if (!long.TryParse(item, out var parsedGroupId) || parsedGroupId <= 0)
                {
                    error = $"Invalid group id `{item}`. Use positive integers, e.g. `group_id:28,32`.";
                    return false;
                }

                parsedGroupIdsToken.Add(parsedGroupId);
            }

            if (parsedGroupIdsToken.Count == 0)
            {
                error = "Group id filter is empty. Use for example `group_id:28,32`.";
                return false;
            }

            groupIds = parsedGroupIdsToken;
        }

        if (feedType == FeedType.Group && rulesets is not null)
        {
            error = "Ruleset filter only applies to `map` feed subscriptions.";
            return false;
        }

        if (feedType == FeedType.Group && eventTypes is not null)
        {
            error = "Event type filter only applies to `map` feed subscriptions.";
            return false;
        }

        if (feedType == FeedType.Map && groupIds is not null)
        {
            error = "Group id filter only applies to `group` feed subscriptions.";
            return false;
        }

        return true;
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
