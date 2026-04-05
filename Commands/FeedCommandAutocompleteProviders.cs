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
