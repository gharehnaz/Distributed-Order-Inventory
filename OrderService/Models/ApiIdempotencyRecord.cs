namespace OrderService.Models;

/// <summary>
/// Stores the result of an idempotent API operation.
/// The key is scoped to an operation (HTTP method + route).
/// </summary>
public sealed class ApiIdempotencyRecord
{
    public long Id { get; set; }
    public string Operation { get; set; } = null!;
    public string Key { get; set; } = null!;
    public string RequestHash { get; set; } = null!;
    public Guid ResourceId { get; set; }
    public int ResponseStatusCode { get; set; }
    public DateTime CreatedAtUtc { get; set; }
}
