using MappingFeed.Common.Services.Beatmap;
using MappingFeed.Discord.Dispatchers.Events;
using Microsoft.Extensions.Logging;
using NetCord;
using NetCord.Rest;
using NetCord.Services.ApplicationCommands;

namespace MappingFeed.Discord.Commands;

/// <summary>
/// Developer-only commands. Registered only when the host runs in the Development environment.
/// </summary>
public sealed class DevCommandModule(
    IBeatmapsetEventsFetcher beatmapsetEventsFetcher,
    BeatmapEventsDispatcher beatmapEventsDispatcher,
    RestClient restClient,
    ILogger<DevCommandModule> logger)
    : ApplicationCommandModule<ApplicationCommandContext>
{
    [SlashCommand("preview-events", "[dev] Fetch a beatmapset's events from osu! and post an embed for each feed-visible one.")]
    public async Task PreviewEventsAsync(
        [SlashCommandParameter(Name = "beatmapset_id", Description = "osu! beatmapset id")]
        long beatmapsetId)
    {
        if (!Context.IsGuildInteraction())
        {
            await RespondAsync(InteractionCallback.Message(
                new InteractionMessageProperties()
                    .WithContent("This command only works in server channels.")
                    .WithFlags(MessageFlags.Ephemeral)));
            return;
        }

        // Sending several embeds can exceed Discord's 3s interaction deadline, so defer first.
        await RespondAsync(InteractionCallback.DeferredMessage(MessageFlags.Ephemeral));

        string summary;
        if (await restClient.GetChannelAsync(Context.Channel.Id) is not TextChannel channel)
        {
            summary = "This channel is not a text channel.";
        }
        else
        {
            int newlyFetched;
            try
            {
                newlyFetched = await beatmapsetEventsFetcher.FetchBeatmapsetAsync(beatmapsetId, CancellationToken.None);
            }
            catch (Exception exception)
            {
                logger.LogError(exception, "Failed fetching beatmapset {SetId} for preview.", beatmapsetId);
                await ModifyResponseAsync(message => message.WithContent(
                    $"Failed to fetch beatmapset `{beatmapsetId}` from osu!: {exception.Message}"));
                return;
            }

            var result = await beatmapEventsDispatcher.PreviewSetAsync(beatmapsetId, channel, CancellationToken.None);
            summary = result.Total == 0
                ? $"No feed events found on osu! for beatmapset `{beatmapsetId}`."
                : $"Beatmapset `{beatmapsetId}`: fetched {newlyFetched} new event(s), {result.Total} total. " +
                  $"Sent {result.Sent}, skipped {result.Skipped} (suppressed or merged), failed {result.Failed}.";
        }

        await ModifyResponseAsync(message => message.WithContent(summary));
    }
}
