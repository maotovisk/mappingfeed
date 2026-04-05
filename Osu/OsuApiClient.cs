using System.Net.Http.Headers;
using System.Text.Json;
using System.Text.Json.Nodes;
using MappingFeed.Config;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Options;

namespace MappingFeed.Osu;

public sealed class OsuApiClient(
    HttpClient httpClient,
    OsuAuthClient authClient,
    IMemoryCache memoryCache,
    IOptions<OsuOptions> osuOptions,
    IOptions<FeedOptions> feedOptions,
    ILogger<OsuApiClient> logger)
{
    private const string BeatmapsetEventsPath = "/api/v2/beatmapsets/events";
    private const string GroupHistoryPath = "/groups/history";

    private readonly string _baseUrl = osuOptions.Value.BaseUrl.TrimEnd('/');
    private readonly TimeSpan _cacheDuration = TimeSpan.FromMinutes(Math.Clamp(feedOptions.Value.ApiCacheMinutes, 5, 20));

    public async Task<OsuBeatmapsetEventsPayload> GetBeatmapsetEventsAsync(int limit, CancellationToken cancellationToken)
    {
        var query = new List<KeyValuePair<string, string?>>
        {
            new("sort", "id_desc"),
            new("limit", limit.ToString()),
        };

        var root = await GetRootAsync(BeatmapsetEventsPath, query, cancellationToken);
        var payload = OsuBeatmapsetEventsParser.Parse(root);

        foreach (var user in payload.Users)
        {
            if (!string.IsNullOrWhiteSpace(user.Username))
                memoryCache.Set($"user:{user.Id}", user.Username, _cacheDuration);
        }

        return payload;
    }

    public async Task<IReadOnlyList<JsonObject>> GetGroupHistoryEventsAsync(int limit, CancellationToken cancellationToken)
    {
        var root = await GetRootAsync(
            GroupHistoryPath,
            [
                new("sort", "id_desc"),
                new("limit", limit > 0 ? limit.ToString() : null),
            ],
            cancellationToken);

        foreach (var group in (root.TryGetArray("groups") ?? []).OfType<JsonObject>())
        {
            var groupId = group.TryGetInt64("id");
            var groupName = group.TryGetString("short_name", "name");

            if (groupId is not null && !string.IsNullOrWhiteSpace(groupName))
                memoryCache.Set($"group:{groupId.Value}", groupName, _cacheDuration);
        }

        var events = ExtractEvents(root, "events");

        foreach (var groupEvent in events)
        {
            var userId = groupEvent.TryGetInt64("user_id");
            var userName = groupEvent.TryGetString("user_name");

            if (userId is not null && !string.IsNullOrWhiteSpace(userName))
                memoryCache.Set($"user:{userId.Value}", userName, _cacheDuration);
        }

        return limit > 0 ? events.Take(limit).ToList() : events;
    }

    public Task<OsuBeatmapsetInfo?> GetBeatmapsetAsync(long setId, CancellationToken cancellationToken)
    {
        return GetCachedAsync($"beatmapset:{setId}", async token =>
        {
            var root = await GetRootAsync($"/api/v2/beatmapsets/{setId}", [], token);
            if (root.Count == 0)
                return null;

            var artist = root.TryGetString("artist");
            var title = root.TryGetString("title");
            var creator = root.TryGetString("creator");
            var displayTitle = string.Join(" - ", new[] { artist, title }.Where(x => !string.IsNullOrWhiteSpace(x)));
            if (string.IsNullOrWhiteSpace(displayTitle))
                displayTitle = title;

            var covers = root["covers"] as JsonObject;
            var cardCoverUrl = covers?.TryGetString("card@2x", "card", "cover@2x", "cover");

            return new OsuBeatmapsetInfo(displayTitle, creator, cardCoverUrl);
        }, cancellationToken);
    }

    public Task<string?> GetUserNameAsync(long userId, CancellationToken cancellationToken)
    {
        return GetCachedAsync($"user:{userId}", async token =>
        {
            var root = await GetRootAsync($"/api/v2/users/{userId}", [], token);
            return root.TryGetString("username");
        }, cancellationToken);
    }

    public Task<OsuUserProfileInfo?> GetUserProfileAsync(long userId, CancellationToken cancellationToken)
    {
        return GetCachedAsync($"user_profile:{userId}", async token =>
        {
            var root = await GetRootAsync($"/api/v2/users/{userId}", [], token);
            if (root.Count == 0)
                return null;

            var badge = ResolveUserBadge(root);

            return new OsuUserProfileInfo(
                root.TryGetString("username"),
                root.TryGetString("avatar_url"),
                badge);
        }, cancellationToken);
    }

    public Task<string?> GetGroupNameAsync(long groupId, CancellationToken cancellationToken)
    {
        return GetCachedAsync($"group:{groupId}", async token =>
        {
            var root = await GetRootAsync($"/api/v2/groups/{groupId}", [], token);
            return root.TryGetString("name", "short_name");
        }, cancellationToken);
    }

    public async Task<string?> GetBeatmapsetDiscussionMessageAsync(
        long setId,
        long? discussionPostId,
        long? discussionId,
        CancellationToken cancellationToken)
    {
        var cacheKey =
            $"beatmapset_discussion_message:set:{setId}:post:{discussionPostId?.ToString() ?? "none"}:discussion:{discussionId?.ToString() ?? "none"}";

        if (memoryCache.TryGetValue(cacheKey, out string? cachedMessage) && !string.IsNullOrWhiteSpace(cachedMessage))
            return cachedMessage;

        string? message = null;

        if (discussionPostId is not null)
            message = await TryGetDiscussionPostMessageAsync(discussionPostId.Value, cancellationToken);

        if (string.IsNullOrWhiteSpace(message) && discussionId is not null)
            message = await TryGetDiscussionMessageAsync(discussionId.Value, cancellationToken);

        if (string.IsNullOrWhiteSpace(message))
            message = await TrySearchDiscussionMessageAsync(setId, discussionPostId, discussionId, cancellationToken);

        if (!string.IsNullOrWhiteSpace(message))
            memoryCache.Set(cacheKey, message, _cacheDuration);

        return message;
    }

    public async Task<string?> GetLatestPraiseOrHypeMessageAsync(
        long setId,
        long userId,
        DateTimeOffset? atOrBefore,
        CancellationToken cancellationToken)
    {
        var timeBucket = atOrBefore?.ToUnixTimeSeconds() / 60;
        var cacheKey = $"beatmapset_praise_hype_message:set:{setId}:user:{userId}:bucket:{timeBucket?.ToString() ?? "none"}";

        if (memoryCache.TryGetValue(cacheKey, out string? cachedMessage) && !string.IsNullOrWhiteSpace(cachedMessage))
            return cachedMessage;

        var root = await GetRootAsync(
            "/api/v2/beatmapsets/discussions",
            [
                new("beatmapset_id", setId.ToString()),
                new("sort", "id_desc"),
                new("limit", "100"),
            ],
            cancellationToken);

        var discussions = (root.TryGetArray("discussions", "beatmapset_discussions", "included_discussions") ?? [])
            .OfType<JsonObject>()
            .Where(x => x.TryGetInt64("user_id") == userId)
            .Where(x => IsPraiseOrHype(x.TryGetString("message_type")))
            .Select(x => new
            {
                Discussion = x,
                CreatedAt = ParseDateTimeOffset(x.TryGetString("created_at")),
            })
            .ToList();

        if (discussions.Count == 0)
            return null;

        var chosen = discussions
            .Where(x => atOrBefore is null || x.CreatedAt is null || x.CreatedAt <= atOrBefore)
            .OrderByDescending(x => x.CreatedAt ?? DateTimeOffset.MinValue)
            .FirstOrDefault()
            ?? discussions.OrderByDescending(x => x.CreatedAt ?? DateTimeOffset.MinValue).FirstOrDefault();

        if (chosen is null)
            return null;

        var message = GetDiscussionMessageFromListing(root, chosen.Discussion);
        if (!string.IsNullOrWhiteSpace(message))
            memoryCache.Set(cacheKey, message, _cacheDuration);

        return message;
    }

    public async Task<IReadOnlyList<string>> GetBeatmapsetModesFailsafeAsync(
        long setId,
        long? preferredUserId,
        DateTimeOffset? atOrBefore,
        CancellationToken cancellationToken)
    {
        var timeBucket = atOrBefore?.ToUnixTimeSeconds() / 60;
        var cacheKey =
            $"beatmapset_modes_failsafe:set:{setId}:user:{preferredUserId?.ToString() ?? "none"}:bucket:{timeBucket?.ToString() ?? "none"}";

        if (memoryCache.TryGetValue(cacheKey, out IReadOnlyList<string>? cachedModes) && cachedModes is not null)
            return cachedModes;

        var modesFromDiscussions = await TryGetModesFromDiscussionsAsync(setId, preferredUserId, atOrBefore, cancellationToken);
        if (modesFromDiscussions.Count > 0)
        {
            memoryCache.Set(cacheKey, modesFromDiscussions, _cacheDuration);
            return modesFromDiscussions;
        }

        var modesFromBeatmapset = await TryGetModesFromBeatmapsetAsync(setId, cancellationToken);
        if (modesFromBeatmapset.Count > 0)
        {
            memoryCache.Set(cacheKey, modesFromBeatmapset, _cacheDuration);
            return modesFromBeatmapset;
        }

        return [];
    }

    public async Task<string?> GetLatestDiscussionMessageByUserAsync(
        long setId,
        long userId,
        DateTimeOffset? atOrBefore,
        CancellationToken cancellationToken)
    {
        var timeBucket = atOrBefore?.ToUnixTimeSeconds() / 60;
        var cacheKey =
            $"beatmapset_discussion_message_failsafe:set:{setId}:user:{userId}:bucket:{timeBucket?.ToString() ?? "none"}";

        if (memoryCache.TryGetValue(cacheKey, out string? cachedMessage) && !string.IsNullOrWhiteSpace(cachedMessage))
            return cachedMessage;

        var root = await GetRootAsync(
            "/api/v2/beatmapsets/discussions",
            [
                new("beatmapset_id", setId.ToString()),
                new("sort", "id_desc"),
                new("limit", "100"),
            ],
            cancellationToken);

        var candidates = (root.TryGetArray("discussions", "beatmapset_discussions", "included_discussions") ?? [])
            .OfType<JsonObject>()
            .Where(x => x.TryGetInt64("user_id") == userId)
            .Where(x => IsUsefulDiscussionMessageType(x.TryGetString("message_type")))
            .Select(x => new
            {
                Discussion = x,
                CreatedAt = ParseDateTimeOffset(x.TryGetString("created_at")),
            })
            .Where(x => atOrBefore is null || x.CreatedAt is null || x.CreatedAt <= atOrBefore)
            .OrderByDescending(x => x.CreatedAt ?? DateTimeOffset.MinValue)
            .ToList();

        foreach (var candidate in candidates)
        {
            var message = GetDiscussionMessageFromListing(root, candidate.Discussion);
            if (string.IsNullOrWhiteSpace(message))
                continue;

            memoryCache.Set(cacheKey, message, _cacheDuration);
            return message;
        }

        return null;
    }

    public async Task<IReadOnlyList<OsuBeatmapsetEventsEvent>> GetCompleteBeatmapsetEventHistoryAsync(
        long setId,
        CancellationToken cancellationToken)
    {
        var cacheKey = $"beatmapset_events_history:set:{setId}";
        if (memoryCache.TryGetValue(cacheKey, out IReadOnlyList<OsuBeatmapsetEventsEvent>? cachedEvents) && cachedEvents is not null)
            return cachedEvents;

        const int pageSize = 50;
        const int maxPages = 30;

        var allEvents = new List<OsuBeatmapsetEventsEvent>();
        var seenEventIds = new HashSet<long>();
        string? cursorString = null;

        for (var page = 0; page < maxPages; page++)
        {
            var query = new List<KeyValuePair<string, string?>>
            {
                new("sort", "id_desc"),
                new("limit", pageSize.ToString()),
                new("beatmapset_id", setId.ToString()),
            };

            if (!string.IsNullOrWhiteSpace(cursorString))
                query.Add(new KeyValuePair<string, string?>("cursor_string", cursorString));

            var root = await GetRootAsync(BeatmapsetEventsPath, query, cancellationToken);
            var payload = OsuBeatmapsetEventsParser.Parse(root);
            if (payload.Events.Count == 0)
                break;

            foreach (var user in payload.Users)
            {
                if (!string.IsNullOrWhiteSpace(user.Username))
                    memoryCache.Set($"user:{user.Id}", user.Username, _cacheDuration);
            }

            foreach (var beatmapsetEvent in payload.Events)
            {
                if (beatmapsetEvent.BeatmapsetId != setId)
                    continue;

                if (!seenEventIds.Add(beatmapsetEvent.Id))
                    continue;

                allEvents.Add(beatmapsetEvent);
            }

            var nextCursorString = root.TryGetString("cursor_string");
            if (string.IsNullOrWhiteSpace(nextCursorString) ||
                string.Equals(nextCursorString, cursorString, StringComparison.Ordinal))
            {
                break;
            }

            cursorString = nextCursorString;
        }

        var result = allEvents
            .OrderBy(x => x.Id)
            .ToList();

        memoryCache.Set(cacheKey, result, _cacheDuration);
        return result;
    }

    private async Task<T?> GetCachedAsync<T>(
        string cacheKey,
        Func<CancellationToken, Task<T?>> valueFactory,
        CancellationToken cancellationToken)
        where T : class
    {
        if (memoryCache.TryGetValue(cacheKey, out T? cached) && cached is not null)
            return cached;

        var value = await valueFactory(cancellationToken);
        if (value is not null)
            memoryCache.Set(cacheKey, value, _cacheDuration);

        return value;
    }

    private async Task<string?> TryGetDiscussionPostMessageAsync(long postId, CancellationToken cancellationToken)
    {
        var root = await GetRootAsync($"/api/v2/beatmapsets/discussions/posts/{postId}", [], cancellationToken);

        return root.TryGetString("message")
            ?? root.TryGetNestedString("post", "message")
            ?? root.TryGetNestedString("beatmapset_discussion_post", "message");
    }

    private async Task<string?> TryGetDiscussionMessageAsync(long discussionId, CancellationToken cancellationToken)
    {
        var root = await GetRootAsync($"/api/v2/beatmapsets/discussions/{discussionId}", [], cancellationToken);

        return root.TryGetNestedString("starting_post", "message")
            ?? root.TryGetNestedString("discussion", "starting_post", "message")
            ?? root.TryGetString("message")
            ?? root.TryGetNestedString("discussion", "message");
    }

    private async Task<string?> TrySearchDiscussionMessageAsync(
        long setId,
        long? discussionPostId,
        long? discussionId,
        CancellationToken cancellationToken)
    {
        var root = await GetRootAsync(
            "/api/v2/beatmapsets/discussions",
            [
                new("beatmapset_id", setId.ToString()),
                new("sort", "id_desc"),
                new("limit", "50"),
            ],
            cancellationToken);

        if (discussionPostId is not null)
        {
            var matchedPost = FindObjectById(root, discussionPostId.Value, "posts", "included_posts", "beatmapset_discussion_posts");
            var postMessage = matchedPost?.TryGetString("message");
            if (!string.IsNullOrWhiteSpace(postMessage))
                return postMessage;
        }

        JsonObject? matchedDiscussion = null;
        if (discussionId is not null)
            matchedDiscussion = FindObjectById(root, discussionId.Value, "discussions", "beatmapset_discussions", "included_discussions");

        matchedDiscussion ??= (root.TryGetArray("discussions", "beatmapset_discussions", "included_discussions") ?? [])
            .OfType<JsonObject>()
            .FirstOrDefault();

        if (matchedDiscussion is null)
            return null;

        var discussionMessage = matchedDiscussion.TryGetNestedString("starting_post", "message")
            ?? matchedDiscussion.TryGetString("message");

        if (!string.IsNullOrWhiteSpace(discussionMessage))
            return discussionMessage;

        var startingPostId = matchedDiscussion.TryGetInt64("starting_post_id");
        if (startingPostId is null)
            return null;

        var startingPost = FindObjectById(root, startingPostId.Value, "posts", "included_posts", "beatmapset_discussion_posts");
        return startingPost?.TryGetString("message");
    }

    private async Task<JsonObject> GetRootAsync(
        string path,
        IEnumerable<KeyValuePair<string, string?>> query,
        CancellationToken cancellationToken)
    {
        using var request = new HttpRequestMessage(HttpMethod.Get, BuildUri(path, query));
        request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", await authClient.GetAccessTokenAsync(cancellationToken));
        request.Headers.Accept.Clear();
        request.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));

        using var response = await httpClient.SendAsync(request, cancellationToken);
        var contentType = response.Content.Headers.ContentType?.MediaType;

        if (!response.IsSuccessStatusCode)
        {
            logger.LogWarning(
                "osu! API request failed for {Path} with status {StatusCode} (content-type: {ContentType}).",
                path,
                (int)response.StatusCode,
                contentType ?? "unknown");
            return [];
        }

        var body = await response.Content.ReadAsStringAsync(cancellationToken);
        if (string.IsNullOrWhiteSpace(body))
            return [];

        JsonNode? jsonNode;
        try
        {
            jsonNode = JsonNode.Parse(body);
        }
        catch (JsonException exception)
        {
            logger.LogWarning(
                exception,
                "osu! API returned invalid JSON for {Path} (status {StatusCode}, content-type: {ContentType}). Body prefix: {Prefix}",
                path,
                (int)response.StatusCode,
                contentType ?? "unknown",
                ToLogPrefix(body));
            return [];
        }

        if (jsonNode is JsonObject jsonObject)
            return jsonObject;

        logger.LogWarning("osu! API returned a non-object payload for {Path}.", path);
        return [];
    }

    private string BuildUri(string path, IEnumerable<KeyValuePair<string, string?>> query)
    {
        var queryString = string.Join(
            "&",
            query
                .Where(x => !string.IsNullOrWhiteSpace(x.Value))
                .Select(x => $"{Uri.EscapeDataString(x.Key)}={Uri.EscapeDataString(x.Value!)}"));

        return string.IsNullOrWhiteSpace(queryString)
            ? $"{_baseUrl}{path}"
            : $"{_baseUrl}{path}?{queryString}";
    }

    private static JsonObject? FindObjectById(JsonObject root, long expectedId, params string[] arrayPropertyNames)
    {
        foreach (var arrayName in arrayPropertyNames)
        {
            if (root[arrayName] is not JsonArray array)
                continue;

            foreach (var node in array.OfType<JsonObject>())
            {
                if (node.TryGetInt64("id") == expectedId)
                    return node;
            }
        }

        return null;
    }

    private static string? GetDiscussionMessageFromListing(JsonObject root, JsonObject discussion)
    {
        var directMessage = discussion.TryGetNestedString("starting_post", "message")
            ?? discussion.TryGetString("message");
        if (!string.IsNullOrWhiteSpace(directMessage))
            return directMessage;

        var startingPostId = discussion.TryGetInt64("starting_post_id");
        if (startingPostId is null)
            return null;

        var post = FindObjectById(root, startingPostId.Value, "posts", "included_posts", "beatmapset_discussion_posts");
        return post?.TryGetString("message");
    }

    private static bool IsPraiseOrHype(string? messageType)
    {
        return messageType?.Trim().ToLowerInvariant() switch
        {
            "praise" => true,
            "hype" => true,
            _ => false,
        };
    }

    private static bool IsUsefulDiscussionMessageType(string? messageType)
    {
        if (string.IsNullOrWhiteSpace(messageType))
            return true;

        return messageType.Trim().ToLowerInvariant() switch
        {
            "resolved" => false,
            _ => true,
        };
    }

    private static DateTimeOffset? ParseDateTimeOffset(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
            return null;

        return DateTimeOffset.TryParse(value, out var parsed)
            ? parsed
            : null;
    }

    private async Task<IReadOnlyList<string>> TryGetModesFromDiscussionsAsync(
        long setId,
        long? preferredUserId,
        DateTimeOffset? atOrBefore,
        CancellationToken cancellationToken)
    {
        var root = await GetRootAsync(
            "/api/v2/beatmapsets/discussions",
            [
                new("beatmapset_id", setId.ToString()),
                new("sort", "id_desc"),
                new("limit", "100"),
            ],
            cancellationToken);

        var beatmapModes = new Dictionary<long, string>();
        foreach (var beatmap in (root.TryGetArray("beatmaps") ?? []).OfType<JsonObject>())
        {
            var beatmapId = beatmap.TryGetInt64("id");
            var mode = TryExtractMode(beatmap);
            if (beatmapId is null || string.IsNullOrWhiteSpace(mode))
                continue;

            beatmapModes[beatmapId.Value] = mode;
        }

        var discussions = (root.TryGetArray("discussions", "included_discussions") ?? [])
            .OfType<JsonObject>()
            .Select(x => new
            {
                Discussion = x,
                BeatmapId = x.TryGetInt64("beatmap_id"),
                UserId = x.TryGetInt64("user_id"),
                CreatedAt = ParseDateTimeOffset(x.TryGetString("created_at")),
            })
            .Where(x => x.BeatmapId is not null)
            .Where(x => atOrBefore is null || x.CreatedAt is null || x.CreatedAt <= atOrBefore)
            .ToList();

        if (preferredUserId is not null)
        {
            var preferredDiscussions = discussions
                .Where(x => x.UserId == preferredUserId.Value)
                .ToList();

            if (preferredDiscussions.Count > 0)
                discussions = preferredDiscussions;
        }

        foreach (var discussion in discussions.OrderByDescending(x => x.CreatedAt ?? DateTimeOffset.MinValue))
        {
            if (discussion.BeatmapId is not long beatmapId)
                continue;

            if (beatmapModes.TryGetValue(beatmapId, out var beatmapMode))
                return [beatmapMode];

            var nestedBeatmapMode = TryExtractMode(discussion.Discussion["beatmap"] as JsonObject);
            if (!string.IsNullOrWhiteSpace(nestedBeatmapMode))
                return [nestedBeatmapMode];
        }

        var distinctModes = beatmapModes.Values
            .Distinct(StringComparer.Ordinal)
            .OrderBy(ModeOrder)
            .ToList();

        return distinctModes;
    }

    private async Task<IReadOnlyList<string>> TryGetModesFromBeatmapsetAsync(
        long setId,
        CancellationToken cancellationToken)
    {
        var root = await GetRootAsync($"/api/v2/beatmapsets/{setId}", [], cancellationToken);
        var modes = (root.TryGetArray("beatmaps") ?? [])
            .OfType<JsonObject>()
            .Select(TryExtractMode)
            .Where(x => !string.IsNullOrWhiteSpace(x))
            .Cast<string>()
            .Distinct(StringComparer.Ordinal)
            .OrderBy(ModeOrder)
            .ToList();

        return modes;
    }

    private static string? TryExtractMode(JsonObject? node)
    {
        if (node is null)
            return null;

        var mode = NormalizeMode(node.TryGetString("mode"));
        if (!string.IsNullOrWhiteSpace(mode))
            return mode;

        var modeInt = node.TryGetInt64("mode_int");
        return modeInt switch
        {
            0 => "osu",
            1 => "taiko",
            2 => "catch",
            3 => "mania",
            _ => null,
        };
    }

    private static string? NormalizeMode(string? mode)
    {
        if (string.IsNullOrWhiteSpace(mode))
            return null;

        return mode.Trim().ToLowerInvariant() switch
        {
            "osu" => "osu",
            "standard" => "osu",
            "taiko" => "taiko",
            "fruits" => "catch",
            "catch" => "catch",
            "catchthebeat" => "catch",
            "mania" => "mania",
            _ => null,
        };
    }

    private static int ModeOrder(string mode)
    {
        return mode switch
        {
            "osu" => 0,
            "taiko" => 1,
            "catch" => 2,
            "mania" => 3,
            _ => 99,
        };
    }

    private static string? ResolveUserBadge(JsonObject root)
    {
        if (root.TryGetArray("groups") is { } groups)
        {
            foreach (var group in groups.OfType<JsonObject>())
            {
                var identifier = group.TryGetString("identifier");
                if (string.Equals(identifier, "bng_limited", StringComparison.OrdinalIgnoreCase))
                    return "PROBATION";

                var shortName = group.TryGetString("short_name");
                if (!string.IsNullOrWhiteSpace(shortName))
                    return shortName;
            }
        }

        var defaultGroup = root.TryGetString("default_group");
        if (string.IsNullOrWhiteSpace(defaultGroup) || defaultGroup.Equals("default", StringComparison.OrdinalIgnoreCase))
            return null;

        return defaultGroup.ToUpperInvariant();
    }

    private static IReadOnlyList<JsonObject> ExtractEvents(JsonObject root, params string[] arrayPropertyNames)
    {
        return (root.TryGetArray(arrayPropertyNames) ?? [])
            .OfType<JsonObject>()
            .ToList();
    }

    private static string ToLogPrefix(string body)
    {
        const int maxLength = 160;
        var trimmed = body.Replace('\r', ' ').Replace('\n', ' ').Trim();
        if (trimmed.Length <= maxLength)
            return trimmed;

        return trimmed[..maxLength] + "...";
    }
}

public sealed record OsuBeatmapsetInfo(string? Title, string? Creator, string? ThumbnailUrl);
public sealed record OsuUserProfileInfo(string? Username, string? AvatarUrl, string? Badge);
