namespace Contracts;

public record InventoryRejected(
	Guid EventId,
	Guid OrderId,
	Guid ProductId,
	int Quantity,
	string Reason,
	DateTime OccurredAtUtc
);