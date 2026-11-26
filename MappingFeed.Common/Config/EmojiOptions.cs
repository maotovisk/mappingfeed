using MappingFeed.Common.Extensions;

namespace MappingFeed.Common.Config;

/// <summary>
/// Emojis used in feed embeds. Any value can be a unicode emoji or a custom Discord
/// emoji (e.g. <c>&lt;:name:id&gt;</c>). Empty values (e.g. unset env vars) keep the defaults.
/// </summary>
public sealed class EmojiOptions
{
    public const string SectionName = "Emojis";

    public string Nomination { get; set => field = value.OrDefault(field); } = "💭";

    public string NominationReset { get; set => field = value.OrDefault(field); } = "🗯️";

    public string Qualification { get; set => field = value.OrDefault(field); } = "❤️";

    public string Disqualification { get; set => field = value.OrDefault(field); } = "💔";

    public string Ranked { get; set => field = value.OrDefault(field); } = "💖";

    public string Unranked { get; set => field = value.OrDefault(field); } = "💥";

    public string GroupAdd { get; set => field = value.OrDefault(field); } = "👾";

    public string GroupRemove { get; set => field = value.OrDefault(field); } = "👾";

    public string GroupMove { get; set => field = value.OrDefault(field); } = "👥";

    public string Fallback { get; set => field = value.OrDefault(field); } = "•";

    // Game mode icons. Empty means the mode is rendered as text, e.g. **[osu]**.
    public string ModeOsu { get; set; } = string.Empty;

    public string ModeTaiko { get; set; } = string.Empty;

    public string ModeCatch { get; set; } = string.Empty;

    public string ModeMania { get; set; } = string.Empty;
}
