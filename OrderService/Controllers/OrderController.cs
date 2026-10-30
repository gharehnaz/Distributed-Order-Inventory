using Contracts;
using Microsoft.AspNetCore.Mvc;
using OrderService.Data;
using OrderService.Models;
using System.Text.Json;


namespace OrderService.Controllers;

[ApiController]
[Route("api/[controller]")]
public class OrdersController : ControllerBase
{
	private readonly OrderDbContext _db;

	public OrdersController(OrderDbContext db)
	{
		_db = db;
	}

	[HttpPost]
	public async Task<IActionResult> Create([FromBody] CreateOrderRequest req, CancellationToken ct)
	{
		if (req.Quantity <= 0)
			return BadRequest("Quantity must be positive.");

		var orderId = Guid.NewGuid();
		var eventId = Guid.NewGuid();

		var now = DateTime.UtcNow;

		var evt = new OrderCreated(
			eventId,
			orderId,
			req.ProductId,
			req.Quantity,
			now);

		var order = new Order
		{
			Id = orderId,
			ProductId = req.ProductId,
			Quantity = req.Quantity,
			CreatedAtUtc = now
		};

		var outboxMessage = new OutboxMessage
		{
			Id = eventId,
			Type = nameof(OrderCreated),
			Payload = JsonSerializer.Serialize(evt),
			OccurredAtUtc = now
		};

		await using var tx = await _db.Database.BeginTransactionAsync(ct);

		_db.Orders.Add(order);
		_db.OutboxMessages.Add(outboxMessage);

		await _db.SaveChangesAsync(ct);
		await tx.CommitAsync(ct);

		return Accepted($"/api/orders/{orderId}", new
		{
			orderId,
			status = "Submitted"
		});
	}
}
