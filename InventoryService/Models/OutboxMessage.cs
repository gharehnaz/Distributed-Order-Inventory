namespace InventoryService.Models;

public class OutboxMessage
{
	public Guid Id { get; set; }

	public string Type { get; set; } = null!;

	public string Payload { get; set; } = null!;

	public DateTime OccurredAtUtc { get; set; }

	public DateTime? ProcessedAtUtc { get; set; }

	public string? LastError { get; set; }
}
