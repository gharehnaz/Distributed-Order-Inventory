namespace InventoryService.Models;

public class OutboxMessage
{
    public Guid Id { get; set; }
    public string Type { get; set; } = null!;
    public string Payload { get; set; } = null!;
    public DateTime OccurredAtUtc { get; set; }
    public DateTime? ProcessedAtUtc { get; set; }

    // Atomic claim / lease
    public string? LockedBy { get; set; }
    public DateTime? LockedUntilUtc { get; set; }

    // Retry policy
    public int RetryCount { get; set; }
    public DateTime? NextRetryAtUtc { get; set; }
    public string? LastError { get; set; }

    // Dead-letter state
    public bool IsDeadLetter { get; set; }
    public DateTime? DeadLetterAtUtc { get; set; }
}
