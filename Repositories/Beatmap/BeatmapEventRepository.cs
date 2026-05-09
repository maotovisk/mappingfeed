using MappingFeed.Data;
using MappingFeed.Data.Entities;
using Microsoft.EntityFrameworkCore;

namespace MappingFeed.Repositories.Beatmap;

public sealed class BeatmapEventRepository(IDbContextFactory<MappingFeedDbContext> dbContextFactory) : IBeatmapEventRepository
{
    public async Task<HashSet<long>> GetExistingEventIdsAsync(
        IReadOnlyCollection<long> eventIds,
        CancellationToken cancellationToken)
    {
        await using var db = await dbContextFactory.CreateDbContextAsync(cancellationToken);

        return await db.BeatmapsetEvents
            .Where(x => eventIds.Contains(x.EventId))
            .Select(x => x.EventId)
            .ToHashSetAsync(cancellationToken);
    }

    public async Task AddRangeAsync(
        IReadOnlyCollection<BeatmapsetEvent> events,
        CancellationToken cancellationToken)
    {
        if (events.Count == 0)
            return;

        await using var db = await dbContextFactory.CreateDbContextAsync(cancellationToken);
        db.BeatmapsetEvents.AddRange(events);
        await db.SaveChangesAsync(cancellationToken);
    }

    public async Task<IReadOnlyList<BeatmapsetEvent>> GetPendingEventsAsync(
        long afterEventId,
        int take,
        CancellationToken cancellationToken)
    {
        await using var db = await dbContextFactory.CreateDbContextAsync(cancellationToken);

        return await db.BeatmapsetEvents
            .Where(x => x.EventId > afterEventId)
            .OrderBy(x => x.EventId)
            .Take(take)
            .ToListAsync(cancellationToken);
    }

    public async Task<bool> HasEarlierNominationAsync(
        long setId,
        long beforeEventId,
        CancellationToken cancellationToken)
    {
        await using var db = await dbContextFactory.CreateDbContextAsync(cancellationToken);

        return await db.BeatmapsetEvents.AnyAsync(
            x => x.EventType == FeedEventType.Nomination
                 && x.SetId == setId
                 && x.EventId < beforeEventId,
            cancellationToken);
    }

    public async Task UpdateAsync(BeatmapsetEvent beatmapsetEvent, CancellationToken cancellationToken)
    {
        await using var db = await dbContextFactory.CreateDbContextAsync(cancellationToken);
        db.BeatmapsetEvents.Update(beatmapsetEvent);
        await db.SaveChangesAsync(cancellationToken);
    }

    public async Task<List<BeatmapsetEvent>> QueryRecentAsync(
        int take,
        long? beforeEventId,
        MapEventsFilter filters,
        CancellationToken cancellationToken)
    {
        await using var db = await dbContextFactory.CreateDbContextAsync(cancellationToken);

        var query = db.BeatmapsetEvents.AsNoTracking();
        if (beforeEventId is not null)
            query = query.Where(x => x.EventId < beforeEventId.Value);
        query = ExcludeSuppressedFromPublicFeed(query);

        if (filters.EventTypes is { Count: > 0 })
        {
            var eventTypes = filters.EventTypes.ToArray();
            query = query.Where(x => eventTypes.Contains(x.EventType));
        }

        if (filters.Ruleset is not null)
        {
            var rulesetToken = filters.Ruleset.Value.ToCommandValue();
            var rulesetMatchToken = $"|{rulesetToken}|";
            query = query.Where(x =>
                x.Rulesets != null &&
                ("|" + x.Rulesets + "|").Contains(rulesetMatchToken));
        }

        if (!string.IsNullOrWhiteSpace(filters.Text))
        {
            var text = filters.Text.Trim();
            var loweredText = text.ToLowerInvariant();
            var hasNumericText = long.TryParse(text, out var parsedNumber);

            query = query.Where(x =>
                (x.BeatmapsetTitle != null && x.BeatmapsetTitle.ToLower().Contains(loweredText)) ||
                (x.MapperName != null && x.MapperName.ToLower().Contains(loweredText)) ||
                (x.ActorUsername != null && x.ActorUsername.ToLower().Contains(loweredText)) ||
                (x.Message != null && x.Message.ToLower().Contains(loweredText)) ||
                (hasNumericText && (
                    x.SetId == parsedNumber ||
                    x.EventId == parsedNumber ||
                    x.TriggeredBy == parsedNumber ||
                    x.MapperUserId == parsedNumber)));
        }

        return await query
            .OrderByDescending(x => x.EventId)
            .Take(take)
            .ToListAsync(cancellationToken);
    }

    private static IQueryable<BeatmapsetEvent> ExcludeSuppressedFromPublicFeed(IQueryable<BeatmapsetEvent> query)
    {
        const long banchoBotUserId = 3;
        const string mapperUpdateResetLikePattern = "%updated by the mapper after a nomination%";

        return query.Where(x =>
            x.EventType != FeedEventType.NominationReset ||
            x.TriggeredBy != banchoBotUserId ||
            !((x.Message != null && EF.Functions.Like(x.Message, mapperUpdateResetLikePattern)) ||
              (x.RawEvent != null && EF.Functions.Like(x.RawEvent, mapperUpdateResetLikePattern))));
    }
}
