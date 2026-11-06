namespace Contracts;

public record InventoryReserved(
	Guid EventId,
	Guid OrderId,
	Guid ProductId,
	int Quantity,
	DateTime OccurredAtUtc
);
