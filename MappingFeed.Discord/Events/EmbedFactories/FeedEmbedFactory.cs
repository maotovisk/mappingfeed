using System.Text.RegularExpressions;
using MappingFeed.Common.Config;
using MappingFeed.Common.Data.Entities;
using MappingFeed.Common.Data.Enums;
using MappingFeed.Common.Data.TransitionalRecords;
using MappingFeed.Common.Extensions;
using MappingFeed.Common.Services.Beatmap;
using MappingFeed.Common.Services.Group;
using Microsoft.Extensions.Options;
using NetCord;
using NetCord.Rest;

namespace MappingFeed.Discord.Events.EmbedFactories;

public sealed partial class FeedEmbedFactory(
    IBeatmapEventService beatmapEventService,
    IGroupEventService groupEventService,
    IOptions<EmojiOptions> emojiOptions)
{
    private readonly EmojiOptions _emojis = emojiOptions.Value;

    public async Task<EmbedProperties> CreateBeatmapsetEventEmbedAsync(
        BeatmapsetEvent beatmapsetEvent,
        CancellationToken cancellationToken)
    {
        var eventView = await beatmapEventService.CreateViewEntryAsync(beatmapsetEvent, cancellationToken);
        return CreateEmbed(eventView);
    }

    public async Task<EmbedProperties> CreateGroupEventEmbedAsync(
        GroupEvent groupEvent,
        CancellationToken cancellationToken)
    {
        var eventView = await groupEventService.CreateViewEntryAsync(groupEvent, cancellationToken);
        return CreateEmbed(eventView);
    }

    private EmbedProperties CreateEmbed(FeedEventViewEntry eventView)
    {
        var embed = new EmbedProperties()
            .WithColor(GetColor(eventView.EventType));

        if (eventView.Map is not null)
            ApplyMapVisuals(embed, eventView, eventView.Map);
        else if (eventView.Group is not null)
            ApplyGroupVisuals(embed, eventView, eventView.Group);

        return embed;
    }

    private void ApplyMapVisuals(
        EmbedProperties embed,
        FeedEventViewEntry eventView,
        FeedMapEventViewData mapData)
    {
        var title = $"{GetEventEmoji(eventView.EventType)}  {GetMapEventDisplayName(eventView.EventType)} ({ToDiscordRelative(eventView.CreatedAt)})";
        var beatmapsetTitleLink = BuildDiscordLink(mapData.BeatmapsetTitle, mapData.BeatmapsetUrl);
        var mapperLink = mapData.MapperUserId is null
            ? EscapeDiscordMarkdown(mapData.MapperName)
            : BuildDiscordLink(mapData.MapperName, $"https://osu.ppy.sh/users/{mapData.MapperUserId.Value}");
        var modeTags = BuildModeEmojis(mapData.Modes.Count == 0 ? ["osu"] : mapData.Modes);

        var lines = new List<string>
        {
            $"**{beatmapsetTitleLink}**",
            $"Mapped by {mapperLink} {modeTags}",
        };

        var rankedHistoryLine = BuildRankedHistoryLine(mapData.RankedHistory, mapData.RankedHistoryEarlierCount);
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
        if (string.IsNullOrWhiteSpace(footerText)) return;
        var footer = new EmbedFooterProperties().WithText(footerText!);
        if (!string.IsNullOrWhiteSpace(eventView.Actor?.AvatarUrl))
            footer.WithIconUrl(eventView.Actor.AvatarUrl!);

        embed.WithFooter(footer);
    }

    private void ApplyGroupVisuals(
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
            $"{BuildDiscordLink(groupData.UserName, groupData.UserUrl)} {relation}",
            $"[**{groupData.GroupName}**]({groupData.GroupUrl})",
        };

        if (groupData.Playmodes.Count > 0)
            lines.Add($"for {BuildModeEmojis(groupData.Playmodes)}");

        embed.WithDescription(string.Join('\n', lines));

        if (!string.IsNullOrWhiteSpace(eventView.Actor?.AvatarUrl))
            embed.WithThumbnail(new EmbedThumbnailProperties(eventView.Actor.AvatarUrl!));
    }

    private string? BuildRankedHistoryLine(IReadOnlyList<FeedMapHistoryAction> rankedHistory, int earlierCount)
    {
        if (rankedHistory.Count == 0)
            return null;

        var parts = rankedHistory
            .Where(x => !string.IsNullOrWhiteSpace(x.Username))
            .Select(x => $"{GetEventEmoji(x.Action)} {BuildUserDisplayName(x.Username!, x.UserId)}")
            .ToList();

        if (parts.Count == 0)
            return null;

        if (earlierCount > 0)
            parts.Insert(0, $"`...{earlierCount} earlier` |");

        return string.Join("  ", parts);
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
        var quoteText = hasQuote ? $"{nameText} \"{message!.Trim().Truncate(53)}\"" : nameText;

        return eventType switch
        {
            FeedEventType.Nomination => quoteText,
            FeedEventType.Qualification => quoteText,
            FeedEventType.Disqualification => quoteText,
            FeedEventType.Unranked => quoteText,
            FeedEventType.NominationReset => quoteText,
            _ => null,
        };
    }

    private static Color GetColor(FeedEventType eventType) => eventType switch
    {
        FeedEventType.Nomination => new Color(52, 152, 219),
        FeedEventType.Qualification => new Color(255, 89, 120),
        FeedEventType.Ranked => new Color(255, 200, 90),
        FeedEventType.NominationReset => new Color(149, 165, 166),
        FeedEventType.Unranked => new Color(230, 85, 0),
        FeedEventType.Disqualification => new Color(180, 20, 40),
        FeedEventType.GroupAdd => new Color(87, 242, 135),
        FeedEventType.GroupRemove => new Color(201, 164, 255),
        FeedEventType.GroupMove => new Color(52, 152, 219),
        _ => new Color(149, 165, 166),
    };

    private string BuildModeEmojis(IEnumerable<string> modes)
    {
        return string.Join(' ', modes.Select(GetModeEmoji).Distinct());
    }

    private string GetModeEmoji(string mode) => (mode.Trim().ToLowerInvariant() switch
    {
        "osu" => _emojis.ModeOsu,
        "taiko" => _emojis.ModeTaiko,
        "catch" or "fruits" or "ctb" => _emojis.ModeCatch,
        "mania" => _emojis.ModeMania,
        _ => null,
    }).OrDefault($"**[{mode}]**");

    private string GetEventEmoji(FeedEventType eventType) => eventType switch
    {
        FeedEventType.Nomination => _emojis.Nomination,
        FeedEventType.NominationReset => _emojis.NominationReset,
        FeedEventType.Qualification => _emojis.Qualification,
        FeedEventType.Disqualification => _emojis.Disqualification,
        FeedEventType.Ranked => _emojis.Ranked,
        FeedEventType.Unranked => _emojis.Unranked,
        FeedEventType.GroupAdd => _emojis.GroupAdd,
        FeedEventType.GroupRemove => _emojis.GroupRemove,
        FeedEventType.GroupMove => _emojis.GroupMove,
        _ => _emojis.Fallback,
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

    /// <summary>
    /// Escapes formatting chars only when they could pair up (e.g. *a*, __a__, ||a||).
    /// A single occurrence, or an underscore inside a word (my_name), can't format anything.
    /// </summary>
    private static string? EscapeDiscordMarkdown(string? text)
    {
        if (string.IsNullOrEmpty(text))
            return text;

        var candidates = FormattingChars().Matches(text);
        return FormattingChars().Replace(text, m => candidates.Count(x => x.Value == m.Value) > 1 ? $"\\{m.Value}" : m.Value);
    }

    private static string BuildDiscordLink(string text, string url) =>
        LinkLabelBreakingChars().IsMatch(text)
            ? $"{EscapeDiscordMarkdown(text)} [↗]({url})"
            : $"[{EscapeDiscordMarkdown(text)}]({url})";

    private static string BuildUserDisplayName(string username, long? userId)
    {
        return userId is null
            ? EscapeDiscordMarkdown(username)!
            : BuildDiscordLink(username, $"https://osu.ppy.sh/users/{userId.Value}");
    }

    // An underscore between two letters/digits is plain text in Discord.
    [GeneratedRegex(@"(?<!\\)(?:[*~`|]|(?<![^\W_])_|_(?![^\W_]))")]
    private static partial Regex FormattingChars();

    [GeneratedRegex(@"[\[\]\\]")]
    private static partial Regex LinkLabelBreakingChars();
}
