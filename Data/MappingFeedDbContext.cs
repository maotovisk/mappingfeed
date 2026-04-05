using MappingFeed.Data.Entities;
using Microsoft.EntityFrameworkCore;

namespace MappingFeed.Data;

public sealed class MappingFeedDbContext(DbContextOptions<MappingFeedDbContext> options) : DbContext(options)
{
    public DbSet<SubscribedChannel> SubscribedChannels => Set<SubscribedChannel>();

    public DbSet<BeatmapsetEvent> BeatmapsetEvents => Set<BeatmapsetEvent>();

    public DbSet<GroupEvent> GroupEvents => Set<GroupEvent>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<SubscribedChannel>(entity =>
        {
            entity.ToTable("subscribed_channels");
            entity.HasKey(x => new { x.ChannelId, x.FeedType });
            entity.Property(x => x.ChannelId).HasColumnName("channel_id");
            entity.Property(x => x.FeedType).HasColumnName("feed_type").HasConversion<string>();
            entity.Property(x => x.LastEventId).HasColumnName("last_event_id");
            entity.Property(x => x.Rulesets).HasColumnName("rulesets");
            entity.Property(x => x.EventTypes).HasColumnName("event_types");
            entity.Property(x => x.GroupId).HasColumnName("group_id");
        });

        modelBuilder.Entity<BeatmapsetEvent>(entity =>
        {
            entity.ToTable("beatmapset_events");
            entity.HasKey(x => x.EventId);
            entity.Property(x => x.EventId).HasColumnName("event_id");
            entity.Property(x => x.SetId).HasColumnName("set_id");
            entity.Property(x => x.TriggeredBy).HasColumnName("triggered_by");
            entity.Property(x => x.EventType).HasColumnName("event_type").HasConversion<string>();
            entity.Property(x => x.Message).HasColumnName("message");
            entity.Property(x => x.PostId).HasColumnName("post_id");
            entity.Property(x => x.RawEvent).HasColumnName("raw_event");
        });

        modelBuilder.Entity<GroupEvent>(entity =>
        {
            entity.ToTable("group_events");
            entity.HasKey(x => x.EventId);
            entity.Property(x => x.EventId).HasColumnName("event_id");
            entity.Property(x => x.UserId).HasColumnName("user_id");
            entity.Property(x => x.EventType).HasColumnName("event_type").HasConversion<string>();
            entity.Property(x => x.GroupId).HasColumnName("group_id");
            entity.Property(x => x.RawEvent).HasColumnName("raw_event");
        });
    }
}
