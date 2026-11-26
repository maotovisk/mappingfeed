namespace MappingFeed.Common.Data.Enums;

public static class BeatmapsetEventClassification
{
    /// <summary>osu! user id of BanchoBot, which performs automated actions such as unranking maps.</summary>
    public const long BanchoBotUserId = 3;

    /// <summary>
    /// Maps an osu! beatmapset event type to a feed event type, or null if the feed ignores it.
    /// </summary>
    /// <remarks>
    /// osu! has no dedicated unrank event: an unrank is a <c>disqualify</c> event triggered by
    /// BanchoBot, so we try to classify it as <see cref="FeedEventType.Unranked"/>.
    /// </remarks>
    public static FeedEventType? FromOsuEventType(string rawType, long? triggeredBy)
    {
        return rawType.Trim().ToLowerInvariant() switch
        {
            "nominate" => FeedEventType.Nomination,
            "nomination_reset" => FeedEventType.NominationReset,
            "qualify" => FeedEventType.Qualification,
            "disqualify" when triggeredBy == BanchoBotUserId => FeedEventType.Unranked,
            "disqualify" => FeedEventType.Disqualification,
            "rank" => FeedEventType.Ranked,
            "unrank" => FeedEventType.Unranked,
            _ => null,
        };
    }
}
