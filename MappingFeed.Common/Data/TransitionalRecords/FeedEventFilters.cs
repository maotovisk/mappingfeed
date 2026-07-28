using MappingFeed.Common.Data.Enums;

namespace MappingFeed.Common.Data.TransitionalRecords;

public sealed record MapEventsFilter(
    Ruleset? Ruleset,
    IReadOnlyCollection<FeedEventType>? EventTypes,
    string? Text);

public sealed record GroupEventsFilter(
    IReadOnlyCollection<long>? GroupIds,
    string? Playmode);
