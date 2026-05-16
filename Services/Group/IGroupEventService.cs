using MappingFeed.Data.Entities;

namespace MappingFeed.Services.Group;

public interface IGroupEventService
{
    Task<IReadOnlyList<GroupEvent>> SaveNewEventsAsync(IReadOnlyCollection<GroupEvent> events, CancellationToken cancellationToken);

    Task<IReadOnlyList<GroupEvent>> GetPendingEventsAsync(long afterEventId, int take, CancellationToken cancellationToken);

    Task<List<GroupEvent>> QueryRecentAsync(
        int take,
        long? beforeEventId,
        GroupEventsFilter filters,
        CancellationToken cancellationToken);

    Task<FeedCursorPage<FeedEventViewEntry>> GetRecentEventsPageAsync(
        int? limit,
        long? beforeEventId,
        GroupEventsFilter filters,
        CancellationToken cancellationToken);

    Task<FeedEventViewEntry> CreateViewEntryAsync(GroupEvent groupEvent, CancellationToken cancellationToken);
}
