using MappingFeed.Common.Data.Entities;
using MappingFeed.Common.Data.TransitionalRecords;

namespace MappingFeed.Common.Repositories.Group;

public interface IGroupEventRepository
{
    Task<HashSet<long>> GetExistingEventIdsAsync(IReadOnlyCollection<long> eventIds, CancellationToken cancellationToken);

    Task AddRangeAsync(IReadOnlyCollection<GroupEvent> events, CancellationToken cancellationToken);

    Task<IReadOnlyList<GroupEvent>> GetPendingEventsAsync(long afterEventId, int take, CancellationToken cancellationToken);

    Task<List<GroupEvent>> QueryRecentAsync(
        int take,
        long? beforeEventId,
        GroupEventsFilter filters,
        CancellationToken cancellationToken);
}
