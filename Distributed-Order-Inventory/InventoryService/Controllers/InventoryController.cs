using InventoryService.Data;
using InventoryService.Models;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;

namespace InventoryService.Controllers;

[ApiController]
[Route("api/[controller]")]
public class InventoryController : ControllerBase
{
	private readonly InventoryDbContext _db;

	public InventoryController(InventoryDbContext db)
	{
		_db = db;
	}

	[HttpPut("{productId:guid}")]
	public async Task<IActionResult> SetInventory(
		Guid productId,
		[FromBody] SetInventoryRequest req,
		CancellationToken ct)
	{
		if (req.Quantity < 0)
			return BadRequest("Quantity cannot be negative.");

		var item = await _db.InventoryItems
			.SingleOrDefaultAsync(x => x.ProductId == productId, ct);

		if (item is null)
		{
			item = new InventoryItem
			{
				Id = Guid.NewGuid(),
				ProductId = productId,
				AvailableQuantity = req.Quantity
			};

			_db.InventoryItems.Add(item);
		}
		else
		{
			item.AvailableQuantity = req.Quantity;
		}

		await _db.SaveChangesAsync(ct);

		return Ok(new
		{
			productId,
			req.Quantity
		});
	}

	[HttpGet("{productId:guid}")]
	public async Task<IActionResult> GetInventory(
		Guid productId,
		CancellationToken ct)
	{
		var item = await _db.InventoryItems
			.AsNoTracking()
			.SingleOrDefaultAsync(x => x.ProductId == productId, ct);

		if (item is null)
			return NotFound();

		return Ok(item);
	}
}