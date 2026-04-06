using NetCord;
using NetCord.Services.ApplicationCommands;
using NetCord.Services.ComponentInteractions;

namespace MappingFeed.Commands;

internal static class ContextExtensions
{
    public static bool IsGuildInteraction(this ApplicationCommandContext context)
    {
        return context.Interaction.GuildId is not null;
    }

    public static ulong? GetGuildId(this ApplicationCommandContext context)
    {
        return context.Interaction.GuildId;
    }

    public static long GetChannelId(this ApplicationCommandContext context)
    {
        return checked((long)context.Channel.Id);
    }

    public static ulong GetUserId(this ApplicationCommandContext context)
    {
        return context.User.Id;
    }

    public static ulong GetUserId(this ComponentInteractionContext context)
    {
        return context.User.Id;
    }

    public static bool TryGetStringMenuInteraction(
        this ComponentInteractionContext context,
        out StringMenuInteraction interaction)
    {
        if (context.Interaction is StringMenuInteraction stringMenuInteraction)
        {
            interaction = stringMenuInteraction;
            return true;
        }

        interaction = null!;
        return false;
    }
}
