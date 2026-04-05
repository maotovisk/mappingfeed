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

public sealed class SubscribeAdditionalFiltersAutocompleteProvider : IAutocompleteProvider<AutocompleteInteractionContext>
{
    private static readonly IReadOnlyList<string> MapFilters =
    [
        "ruleset:osu",
        "ruleset:taiko",
        "ruleset:catch",
        "ruleset:mania",
        "ruleset:osu,mania",
        "event_type:nominate",
        "event_type:nomination_reset",
        "event_type:qualify",
        "event_type:disqualify",
        "event_type:rank",
        "event_type:unrank",
        "event_type:rank,qualify",
    ];

    private static readonly IReadOnlyList<string> GroupFilters =
    [
        "group_id:7",
        "group_id:11",
        "group_id:16",
        "group_id:28",
        "group_id:31",
        "group_id:32",
        "group_id:35",
        "group_id:48",
        "group_id:50",
    ];

    public ValueTask<IEnumerable<ApplicationCommandOptionChoiceProperties>?> GetChoicesAsync(
        ApplicationCommandInteractionDataOption option,
        AutocompleteInteractionContext context)
    {
        var feedTypeValue = context.Interaction.Data.Options
            .FirstOrDefault(x => string.Equals(x.Name, "type", StringComparison.OrdinalIgnoreCase))
            ?.Value;

        var source = string.Equals(feedTypeValue?.Trim(), "group", StringComparison.OrdinalIgnoreCase)
            ? GroupFilters
            : MapFilters;

        var input = option.Value?.Trim() ?? string.Empty;
        var filtered = source
            .Where(x => string.IsNullOrWhiteSpace(input) || x.Contains(input, StringComparison.OrdinalIgnoreCase))
            .Take(25)
            .Select(x => new ApplicationCommandOptionChoiceProperties(x, x))
            .ToList();

        return new ValueTask<IEnumerable<ApplicationCommandOptionChoiceProperties>?>(filtered);
    }
}
