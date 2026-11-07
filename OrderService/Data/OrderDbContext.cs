using Microsoft.EntityFrameworkCore;
using OrderService.Models;
namespace OrderService.Data;
public sealed class OrderDbContext(DbContextOptions<OrderDbContext> options) : DbContext(options)
{
    public DbSet<Order> Orders => Set<Order>();
    public DbSet<OutboxMessage> OutboxMessages => Set<OutboxMessage>();
    public DbSet<ApiIdempotencyRecord> ApiIdempotencyRecords => Set<ApiIdempotencyRecord>();
	public DbSet<ProcessedMessage> ProcessedMessages
	=> Set<ProcessedMessage>();
	protected override void OnModelCreating(ModelBuilder b)
    {
        b.Entity<Order>().ToTable("Orders").HasKey(x => x.Id);
        b.Entity<Order>().ToTable(t => t.HasCheckConstraint("CK_Order_Quantity", "[Quantity] > 0"));
        b.Entity<OutboxMessage>(e =>
        {
            e.ToTable("OutboxMessages"); e.HasKey(x => x.Id);
            e.Property(x => x.Type).HasMaxLength(200).IsRequired();
            e.Property(x => x.Payload).HasColumnType("nvarchar(max)").IsRequired();
            e.Property(x => x.LastError).HasMaxLength(2000);
            e.Property(x => x.LockedBy).HasMaxLength(100);
            e.HasIndex(x => new { x.PublishedAtUtc, x.IsDeadLetter, x.NextRetryAtUtc, x.OccurredAtUtc });
        });
		b.Entity<ProcessedMessage>(entity =>
		{
			entity.HasKey(x => x.EventId);
		});
        b.Entity<ApiIdempotencyRecord>(entity =>
        {
            entity.ToTable("ApiIdempotencyRecords");
            entity.HasKey(x => x.Id);
            entity.Property(x => x.Operation).HasMaxLength(100).IsRequired();
            entity.Property(x => x.Key).HasMaxLength(128).UseCollation("Latin1_General_100_BIN2").IsRequired();
            entity.Property(x => x.RequestHash).HasMaxLength(64).IsRequired();
            entity.HasIndex(x => new { x.Operation, x.Key }).IsUnique()
                .HasDatabaseName("UX_ApiIdempotencyRecords_Operation_Key");
        });
	}
}
