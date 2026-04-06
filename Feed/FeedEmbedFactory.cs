using MappingFeed.Data.Entities;
using NetCord;
using NetCord.Rest;

namespace MappingFeed.Feed;

public sealed class FeedEmbedFactory(FeedEventViewFactory eventViewFactory)
{
    public async Task<EmbedProperties> CreateBeatmapsetEventEmbedAsync(
        BeatmapsetEvent beatmapsetEvent,
        CancellationToken cancellationToken)
    {
        var eventView = await eventViewFactory.CreateBeatmapsetEventEntryAsync(beatmapsetEvent, cancellationToken);
        return CreateEmbed(eventView);
    }

    public async Task<EmbedProperties> CreateGroupEventEmbedAsync(
        GroupEvent groupEvent,
        CancellationToken cancellationToken)
    {
        var eventView = await eventViewFactory.CreateGroupEventEntryAsync(groupEvent, cancellationToken);
        return CreateEmbed(eventView);
    }

    private static EmbedProperties CreateEmbed(FeedEventViewEntry eventView)
    {
        var color = eventView.Visual.Color;
        var embed = new EmbedProperties()
            .WithColor(new Color(color.R, color.G, color.B));

        if (!string.IsNullOrWhiteSpace(eventView.Visual.Title))
        {
            var value = eventView.Visual.Lines.Count == 0
                ? "-"
                : string.Join('\n', eventView.Visual.Lines);

            embed.WithFields([
                new EmbedFieldProperties()
                    .WithName(eventView.Visual.Title)
                    .WithValue(value)
            ]);
        }
        else if (!string.IsNullOrWhiteSpace(eventView.Visual.Description))
        {
            embed.WithDescription(eventView.Visual.Description);
        }

        if (!string.IsNullOrWhiteSpace(eventView.Visual.ThumbnailUrl))
            embed.WithThumbnail(new EmbedThumbnailProperties(eventView.Visual.ThumbnailUrl));

        if (!string.IsNullOrWhiteSpace(eventView.Visual.FooterText))
        {
            var footer = new EmbedFooterProperties().WithText(eventView.Visual.FooterText!);
            if (!string.IsNullOrWhiteSpace(eventView.Actor?.AvatarUrl))
                footer.WithIconUrl(eventView.Actor.AvatarUrl!);

            embed.WithFooter(footer);
        }

        return embed;
    }
}
