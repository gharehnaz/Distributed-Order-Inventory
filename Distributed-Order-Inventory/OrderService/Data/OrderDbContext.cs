using Microsoft.EntityFrameworkCore;
using OrderService.Models;
using System.Reflection.Emit;
namespace OrderService.Data;
public sealed class OrderDbContext(DbContextOptions<OrderDbContext> options) : DbContext(options)
{
    public DbSet<Order> Orders => Set<Order>();
    public DbSet<OutboxMessage> OutboxMessages => Set<OutboxMessage>();
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
            e.HasIndex(x => new { x.PublishedAtUtc, x.OccurredAtUtc });
        });
		b.Entity<ProcessedMessage>(entity =>
		{
			entity.HasKey(x => x.EventId);
		});
	}
}
