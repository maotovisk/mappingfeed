using MappingFeed.Data.Entities;

namespace MappingFeed.Repositories.Beatmap;

public interface IBeatmapEventRepository
{
    Task<HashSet<long>> GetExistingEventIdsAsync(IReadOnlyCollection<long> eventIds, CancellationToken cancellationToken);

    Task AddRangeAsync(IReadOnlyCollection<BeatmapsetEvent> events, CancellationToken cancellationToken);

    Task<IReadOnlyList<BeatmapsetEvent>> GetPendingEventsAsync(long afterEventId, int take, CancellationToken cancellationToken);

    Task<bool> HasEarlierNominationAsync(long setId, long beforeEventId, CancellationToken cancellationToken);

    Task UpdateAsync(BeatmapsetEvent beatmapsetEvent, CancellationToken cancellationToken);

    Task<List<BeatmapsetEvent>> QueryRecentAsync(
        int take,
        long? beforeEventId,
        MapEventsFilter filters,
        CancellationToken cancellationToken);
}
