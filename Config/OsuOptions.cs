namespace MappingFeed.Config;

public sealed class OsuOptions
{
    public const string SectionName = "Osu";

    public string BaseUrl { get; set; } = "https://osu.ppy.sh";

    public int ClientId { get; set; }

    public string ClientSecret { get; set; } = string.Empty;
}
