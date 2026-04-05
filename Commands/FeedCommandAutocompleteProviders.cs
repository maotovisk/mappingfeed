using MappingFeed.Feed;
using NetCord;
using NetCord.Rest;
using NetCord.Services.ApplicationCommands;

namespace MappingFeed.Commands;

public sealed class FeedTypeAutocompleteProvider : IAutocompleteProvider<AutocompleteInteractionContext>
{
    private static readonly IReadOnlyList<ApplicationCommandOptionChoiceProperties> Choices =
    [
        new("map", "map"),
        new("group", "group"),
    ];

    public ValueTask<IEnumerable<ApplicationCommandOptionChoiceProperties>?> GetChoicesAsync(
        ApplicationCommandInteractionDataOption option,
        AutocompleteInteractionContext context)
    {
        var input = option.Value?.Trim() ?? string.Empty;
        var filtered = Choices
            .Where(x => string.IsNullOrWhiteSpace(input) ||
                        (x.StringValue?.Contains(input, StringComparison.OrdinalIgnoreCase) ?? false))
            .Take(25)
            .ToList();

        return new ValueTask<IEnumerable<ApplicationCommandOptionChoiceProperties>?>(filtered);
    }
}

public sealed class SubscribeRulesetAutocompleteProvider : IAutocompleteProvider<AutocompleteInteractionContext>
{
    private static readonly IReadOnlyList<string> RulesetChoices =
    [
        "osu",
        "taiko",
        "catch",
        "mania",
    ];

    public ValueTask<IEnumerable<ApplicationCommandOptionChoiceProperties>?> GetChoicesAsync(
        ApplicationCommandInteractionDataOption option,
        AutocompleteInteractionContext context)
    {
        if (AutocompleteHelpers.IsFeedType(context, FeedType.Group))
            return new ValueTask<IEnumerable<ApplicationCommandOptionChoiceProperties>?>([]);

        return AutocompleteHelpers.FilterChoices(option, RulesetChoices);
    }
}

public sealed class SubscribeEventTypeAutocompleteProvider : IAutocompleteProvider<AutocompleteInteractionContext>
{
    private static readonly IReadOnlyList<string> EventTypeChoices =
    [
        "nominate",
        "nomination_reset",
        "qualify",
        "disqualify",
        "rank",
        "unrank"
    ];

    public ValueTask<IEnumerable<ApplicationCommandOptionChoiceProperties>?> GetChoicesAsync(
        ApplicationCommandInteractionDataOption option,
        AutocompleteInteractionContext context)
    {
        if (AutocompleteHelpers.IsFeedType(context, FeedType.Group))
            return new ValueTask<IEnumerable<ApplicationCommandOptionChoiceProperties>?>([]);

        return AutocompleteHelpers.FilterChoices(option, EventTypeChoices);
    }
}

public sealed class SubscribeGroupIdAutocompleteProvider : IAutocompleteProvider<AutocompleteInteractionContext>
{
    private static readonly IReadOnlyList<string> GroupIdChoices =
    [
        "7",
        "11",
        "16",
        "28",
        "31",
        "32",
        "35",
        "48",
        "50",
    ];

    public ValueTask<IEnumerable<ApplicationCommandOptionChoiceProperties>?> GetChoicesAsync(
        ApplicationCommandInteractionDataOption option,
        AutocompleteInteractionContext context)
    {
        if (AutocompleteHelpers.IsFeedType(context, FeedType.Map))
            return new ValueTask<IEnumerable<ApplicationCommandOptionChoiceProperties>?>([]);

        return AutocompleteHelpers.FilterChoices(option, GroupIdChoices);
    }
}

internal static class AutocompleteHelpers
{
    private static readonly char[] CsvSeparators = [',', '|'];

    public static bool IsFeedType(AutocompleteInteractionContext context, FeedType targetType)
    {
        var feedTypeValue = context.Interaction.Data.Options
            .FirstOrDefault(x => string.Equals(x.Name, "type", StringComparison.OrdinalIgnoreCase))
            ?.Value;

        return FeedEnumExtensions.TryParseFeedType(feedTypeValue, out var feedType) && feedType == targetType;
    }

    public static ValueTask<IEnumerable<ApplicationCommandOptionChoiceProperties>?> FilterChoices(
        ApplicationCommandInteractionDataOption option,
        IReadOnlyList<string> source)
    {
        var input = option.Value ?? string.Empty;
        var selections = ParseCsvSelections(input);
        var selectedValues = selections.selectedValues;
        var currentToken = selections.currentToken;
        var prefix = selectedValues.Count == 0
            ? string.Empty
            : string.Join(",", selectedValues) + ",";

        var filtered = source
            .Where(x => !selectedValues.Contains(x, StringComparer.OrdinalIgnoreCase))
            .Where(x => string.IsNullOrWhiteSpace(currentToken) || x.Contains(currentToken, StringComparison.OrdinalIgnoreCase))
            .Take(25)
            .Select(x =>
            {
                var value = string.IsNullOrEmpty(prefix)
                    ? x
                    : prefix + x;
                return new ApplicationCommandOptionChoiceProperties(value, value);
            })
            .ToList();

        return new ValueTask<IEnumerable<ApplicationCommandOptionChoiceProperties>?>(filtered);
    }

    private static (List<string> selectedValues, string currentToken) ParseCsvSelections(string input)
    {
        if (string.IsNullOrWhiteSpace(input))
            return ([], string.Empty);

        var parts = input.Split(CsvSeparators, StringSplitOptions.None);
        if (parts.Length == 1)
            return ([], parts[0].Trim());

        var selectedValues = parts
            .Take(parts.Length - 1)
            .Select(x => x.Trim())
            .Where(x => !string.IsNullOrWhiteSpace(x))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToList();

        var currentToken = parts[^1].Trim();
        return (selectedValues, currentToken);
    }
}
