using MappingFeed.Common.Data.Entities;
using MappingFeed.Common.Data.TransitionalRecords;

namespace MappingFeed.Common.Services.Beatmap;

public interface IBeatmapEventService
{
    Task<IReadOnlyList<BeatmapsetEvent>> SaveNewEventsAsync(IReadOnlyCollection<BeatmapsetEvent> events, CancellationToken cancellationToken);

    Task<IReadOnlyList<BeatmapsetEvent>> GetPendingEventsAsync(long afterEventId, int take, CancellationToken cancellationToken);

    Task<bool> HasEarlierNominationAsync(long setId, long beforeEventId, CancellationToken cancellationToken);

    Task UpdateAsync(BeatmapsetEvent beatmapsetEvent, CancellationToken cancellationToken);

    Task<List<BeatmapsetEvent>> QueryRecentAsync(
        int take,
        long? beforeEventId,
        MapEventsFilter filters,
        CancellationToken cancellationToken);

    Task<FeedCursorPage<FeedEventViewEntry>> GetRecentEventsPageAsync(
        int? limit,
        long? beforeEventId,
        MapEventsFilter filters,
        CancellationToken cancellationToken);

    Task<FeedEventViewEntry> CreateViewEntryAsync(BeatmapsetEvent beatmapsetEvent, CancellationToken cancellationToken);
}
