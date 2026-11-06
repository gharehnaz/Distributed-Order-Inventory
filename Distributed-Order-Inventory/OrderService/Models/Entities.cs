namespace OrderService.Models;
public sealed class Order
{
    public Guid Id { get; set; }
    public Guid ProductId { get; set; }
    public int Quantity { get; set; }
    public DateTime CreatedAtUtc { get; set; }
	public OrderStatus Status { get; set; }
	public string? RejectionReason { get; set; }
}
public enum OrderStatus
{
	Pending = 0,
	Confirmed = 1,
	Rejected = 2
}
