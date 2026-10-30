namespace InventoryService.Models;

public sealed class ProcessedMessage
{
    public Guid Id { get; set; }
    public Guid EventId { get; set; }
    public DateTime ProcessedAtUtc { get; set; }
}
