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
        var embed = new EmbedProperties()
            .WithColor(GetColor(eventView.EventType));

        if (eventView.Map is not null)
            ApplyMapVisuals(embed, eventView, eventView.Map);
        else if (eventView.Group is not null)
            ApplyGroupVisuals(embed, eventView, eventView.Group);

        return embed;
    }

    private static void ApplyMapVisuals(
        EmbedProperties embed,
        FeedEventViewEntry eventView,
        FeedMapEventViewData mapData)
    {
        var title = $"{GetEventEmoji(eventView.EventType)}  {GetMapEventDisplayName(eventView.EventType)} ({ToDiscordRelative(eventView.CreatedAt)})";
        var mapperLink = mapData.MapperUserId is null
            ? mapData.MapperName
            : $"[{mapData.MapperName}](https://osu.ppy.sh/users/{mapData.MapperUserId.Value})";
        var modeTags = mapData.Modes.Count == 0
            ? "[osu]"
            : string.Join(string.Empty, mapData.Modes.Select(x => $"[{x}]"));

        var lines = new List<string>
        {
            $"**[{mapData.BeatmapsetTitle}]({mapData.BeatmapsetUrl})**",
            $"Mapped by {mapperLink} **{modeTags}**",
        };

        var rankedHistoryLine = BuildRankedHistoryLine(mapData.RankedHistory);
        if (!string.IsNullOrWhiteSpace(rankedHistoryLine))
            lines.Add($"\n{rankedHistoryLine}");

        var fieldValue = lines.Count == 0
            ? "-"
            : string.Join('\n', lines);
        embed.WithFields([
            new EmbedFieldProperties()
                .WithName(title)
                .WithValue(fieldValue)
        ]);

        embed.WithThumbnail(new EmbedThumbnailProperties(BuildBeatmapsetThumbnailUrl(mapData.SetId)));

        var footerText = BuildMapFooterText(eventView.EventType, eventView.Actor, mapData.Message);
        if (!string.IsNullOrWhiteSpace(footerText))
        {
            var footer = new EmbedFooterProperties().WithText(footerText!);
            if (!string.IsNullOrWhiteSpace(eventView.Actor?.AvatarUrl))
                footer.WithIconUrl(eventView.Actor.AvatarUrl!);

            embed.WithFooter(footer);
        }
    }

    private static void ApplyGroupVisuals(
        EmbedProperties embed,
        FeedEventViewEntry eventView,
        FeedGroupEventViewData groupData)
    {
        var relation = eventView.EventType switch
        {
            FeedEventType.GroupAdd => "to the",
            FeedEventType.GroupRemove => "from the",
            _ => "in",
        };
        var label = eventView.EventType switch
        {
            FeedEventType.GroupAdd => "Added",
            FeedEventType.GroupRemove => "Removed",
            _ => eventView.EventType.ToDisplayName(),
        };

        var lines = new List<string>
        {
            $"{GetEventEmoji(eventView.EventType)} **{label}** ({ToDiscordRelative(eventView.CreatedAt)})",
            $"[{groupData.UserName}]({groupData.UserUrl}) {relation}",
            $"[**{groupData.GroupName}**]({groupData.GroupUrl})",
        };

        if (groupData.Playmodes.Count > 0)
            lines.Add($"for [{string.Join(", ", groupData.Playmodes)}]");

        embed.WithDescription(string.Join('\n', lines));

        if (!string.IsNullOrWhiteSpace(eventView.Actor?.AvatarUrl))
            embed.WithThumbnail(new EmbedThumbnailProperties(eventView.Actor.AvatarUrl!));
    }

    private static string? BuildRankedHistoryLine(IReadOnlyList<FeedMapHistoryAction> rankedHistory)
    {
        if (rankedHistory.Count == 0)
            return null;

        var parts = rankedHistory
            .Where(x => !string.IsNullOrWhiteSpace(x.Username))
            .Select(x => $"{GetEventEmoji(x.Action)} {x.Username}")
            .ToList();

        return parts.Count == 0 ? null : string.Join("  ", parts);
    }

    private static string? BuildMapFooterText(
        FeedEventType eventType,
        FeedEventActor? actor,
        string? message)
    {
        var actorName = actor?.Username;
        if (string.IsNullOrWhiteSpace(actorName))
            return null;

        var nameText = actorName.Trim();
        var hasQuote = !string.IsNullOrWhiteSpace(message);
        var quoteText = hasQuote ? $"{nameText} \"{Trim(message!.Trim(), 53)}\"" : nameText;

        return eventType switch
        {
            FeedEventType.Nomination => quoteText,
            FeedEventType.Qualification => quoteText,
            FeedEventType.Disqualification => quoteText,
            FeedEventType.NominationReset => quoteText,
            _ => null,
        };
    }

    private static Color GetColor(FeedEventType eventType) => eventType switch
    {
        FeedEventType.Nomination => new Color(52, 152, 219),
        FeedEventType.Qualification => new Color(255, 89, 120),
        FeedEventType.Ranked => new Color(47, 208, 130),
        FeedEventType.NominationReset => new Color(149, 165, 166),
        FeedEventType.Unranked => new Color(230, 126, 34),
        FeedEventType.Disqualification => new Color(255, 161, 38),
        FeedEventType.GroupAdd => new Color(87, 242, 135),
        FeedEventType.GroupRemove => new Color(201, 164, 255),
        FeedEventType.GroupMove => new Color(52, 152, 219),
        _ => new Color(149, 165, 166),
    };

    private static string GetEventEmoji(FeedEventType eventType) => eventType switch
    {
        FeedEventType.Nomination => "💬",
        FeedEventType.NominationReset => "↩️",
        FeedEventType.Qualification => "❤️",
        FeedEventType.Disqualification => "💔",
        FeedEventType.Ranked => "💖",
        FeedEventType.Unranked => "🟠",
        FeedEventType.GroupAdd => "👾",
        FeedEventType.GroupRemove => "👾",
        FeedEventType.GroupMove => "👥",
        _ => "•",
    };

    private static string GetMapEventDisplayName(FeedEventType eventType) => eventType switch
    {
        FeedEventType.Nomination => "Nominated",
        FeedEventType.Qualification => "Qualified",
        FeedEventType.Disqualification => "Disqualified",
        FeedEventType.NominationReset => "Nomination Reset",
        FeedEventType.Ranked => "Ranked",
        FeedEventType.Unranked => "Unranked",
        _ => eventType.ToDisplayName(),
    };

    private static string ToDiscordRelative(DateTimeOffset? createdAt)
    {
        var timestamp = createdAt ?? DateTimeOffset.UtcNow;
        return $"<t:{timestamp.ToUnixTimeSeconds()}:R>";
    }

    private static string BuildBeatmapsetThumbnailUrl(long setId)
    {
        return $"https://b.ppy.sh/thumb/{setId}l.jpg";
    }

    private static string Trim(string value, int maxLength)
    {
        if (value.Length <= maxLength)
            return value;

        return value[..Math.Max(0, maxLength - 3)] + "...";
    }
}
