namespace MappingFeed.Common.Data.TransitionalRecords;

public sealed record OsuBeatmapsetInfo(string? Title, string? Creator, string? ThumbnailUrl);

public sealed record OsuUserProfileInfo(string? Username, string? AvatarUrl, string? Badge, string? Color);

public sealed record OsuGroupInfo(string? Name, string? Color);
