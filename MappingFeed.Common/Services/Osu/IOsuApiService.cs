using System.Text.Json.Nodes;
using MappingFeed.Common.Data.TransitionalRecords;

namespace MappingFeed.Common.Services.Osu;

public interface IOsuApiService
{
    Task<OsuBeatmapsetEventsPayload> GetBeatmapEventsAsync(int limit, CancellationToken cancellationToken);

    Task<IReadOnlyList<JsonObject>> GetGroupEventsAsync(int limit, CancellationToken cancellationToken);

    Task<OsuBeatmapsetInfo?> GetBeatmapAsync(long setId, CancellationToken cancellationToken);

    Task<OsuUserProfileInfo?> GetUserAsync(long userId, CancellationToken cancellationToken);

    Task<string?> GetUserNameAsync(long userId, CancellationToken cancellationToken);

    Task<string?> GetGroupNameAsync(long groupId, CancellationToken cancellationToken);

    Task<string?> GetGroupColorAsync(long groupId, CancellationToken cancellationToken);

    Task<OsuGroupInfo?> GetGroupInfoAsync(long groupId, CancellationToken cancellationToken);

    Task<IReadOnlyList<string>> GetUserGroupPlaymodesAsync(
        long userId,
        long groupId,
        CancellationToken cancellationToken);

    Task<string?> GetBeatmapsetDiscussionMessageAsync(
        long setId,
        long? discussionPostId,
        long? discussionId,
        CancellationToken cancellationToken);

    Task<string?> GetLatestPraiseOrHypeMessageAsync(
        long setId,
        long userId,
        DateTimeOffset? atOrBefore,
        CancellationToken cancellationToken);

    Task<IReadOnlyList<string>> GetBeatmapsetModesFailsafeAsync(
        long setId,
        long? preferredUserId,
        DateTimeOffset? atOrBefore,
        CancellationToken cancellationToken);

    Task<string?> GetLatestDiscussionMessageByUserAsync(
        long setId,
        long userId,
        DateTimeOffset? atOrBefore,
        CancellationToken cancellationToken);

    Task<IReadOnlyList<OsuBeatmapsetEventsEvent>> GetCompleteBeatmapsetEventHistoryAsync(
        long setId,
        CancellationToken cancellationToken);
}
