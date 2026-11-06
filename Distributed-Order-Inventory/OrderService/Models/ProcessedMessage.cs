namespace OrderService.Models;

public class ProcessedMessage
{
	public Guid EventId { get; set; }
	public DateTime ProcessedAtUtc { get; set; }
}
