using InventoryService.Models;
using Microsoft.EntityFrameworkCore;
using System.Reflection.Emit;
namespace InventoryService.Data;
public sealed class InventoryDbContext(DbContextOptions<InventoryDbContext> options) : DbContext(options)
{
    public DbSet<InventoryItem> InventoryItems => Set<InventoryItem>();
    public DbSet<ProcessedMessage> ProcessedMessages => Set<ProcessedMessage>();
	public DbSet<OutboxMessage> OutboxMessages =>
	Set<OutboxMessage>();
	protected override void OnModelCreating(ModelBuilder b)
    {
        b.Entity<InventoryItem>(e =>
        {
            e.ToTable("InventoryItems"); e.HasKey(x => x.Id);
            e.HasIndex(x => x.ProductId).IsUnique();
            e.ToTable(t => t.HasCheckConstraint("CK_Inventory_NonNegative", "[AvailableQuantity] >= 0"));
        });
        b.Entity<ProcessedMessage>(e =>
        {
            e.ToTable("ProcessedMessages"); e.HasKey(x => x.Id);
            e.HasIndex(x => x.EventId).IsUnique();
        });
		b.Entity<OutboxMessage>(entity =>
		{
			entity.HasKey(x => x.Id);

			entity.Property(x => x.Type)
				.HasMaxLength(200)
				.IsRequired();

			entity.Property(x => x.Payload)
				.IsRequired();

            entity.Property(x => x.LastError).HasMaxLength(2000);
            entity.Property(x => x.LockedBy).HasMaxLength(100);
            entity.HasIndex(x => new { x.ProcessedAtUtc, x.IsDeadLetter, x.NextRetryAtUtc, x.OccurredAtUtc });
		});
	}

}
