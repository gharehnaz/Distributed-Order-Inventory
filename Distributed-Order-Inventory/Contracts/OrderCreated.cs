namespace Contracts;
public sealed record OrderCreated(Guid EventId, Guid OrderId, Guid ProductId, int Quantity, DateTime OccurredAtUtc);
