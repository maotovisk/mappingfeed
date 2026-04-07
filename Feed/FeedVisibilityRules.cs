using MappingFeed.Data.Entities;
using Microsoft.EntityFrameworkCore;

namespace MappingFeed.Feed;

public static class FeedVisibilityRules
{
    private const long BanchoBotUserId = 3;
    private const string MapperUpdateResetMarker = "updated by the mapper after a nomination";
    private const string MapperUpdateResetLikePattern = $"%{MapperUpdateResetMarker}%";

    public static bool ShouldSuppressFromPublicFeed(BeatmapsetEvent beatmapsetEvent)
    {
        if (beatmapsetEvent.EventType != FeedEventType.NominationReset)
            return false;

        if (beatmapsetEvent.TriggeredBy != BanchoBotUserId)
            return false;

        return ContainsIgnoreCase(beatmapsetEvent.Message, MapperUpdateResetMarker)
               || ContainsIgnoreCase(beatmapsetEvent.RawEvent, MapperUpdateResetMarker);
    }

    public static IQueryable<BeatmapsetEvent> ExcludeSuppressedFromPublicFeed(this IQueryable<BeatmapsetEvent> query)
    {
        return query.Where(x =>
            x.EventType != FeedEventType.NominationReset ||
            x.TriggeredBy != BanchoBotUserId ||
            !((x.Message != null && EF.Functions.Like(x.Message, MapperUpdateResetLikePattern)) ||
              (x.RawEvent != null && EF.Functions.Like(x.RawEvent, MapperUpdateResetLikePattern))));
    }

    private static bool ContainsIgnoreCase(string? value, string marker)
    {
        if (string.IsNullOrWhiteSpace(value))
            return false;

        return value.Contains(marker, StringComparison.OrdinalIgnoreCase);
    }
}
