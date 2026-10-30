public sealed record OrderIdempotentTest(Guid EventId, Guid OrderId, int ProductId, int Quantity);
