namespace MappingFeed.Common.Services.Beatmap;

public interface IBeatmapsetEventsFetcher
{
    /// <summary>
    /// Fetches the complete event history of one beatmapset from osu!, enriches it like the
    /// regular poller does, and stores any events not already in the database.
    /// </summary>
    /// <returns>The number of newly stored events.</returns>
    Task<int> FetchBeatmapsetAsync(long setId, CancellationToken cancellationToken);
}
