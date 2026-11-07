using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Contracts;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using OrderService.Data;
using OrderService.Models;

namespace OrderService.Controllers;

[ApiController]
[Route("api/[controller]")]
public sealed class OrdersController : ControllerBase
{
    private const string CreateOrderOperation = "POST:/api/orders";
    private readonly OrderDbContext _db;

    public OrdersController(OrderDbContext db) => _db = db;

    /// <summary>
    /// Creates an order exactly once for a given Idempotency-Key and request payload.
    /// </summary>
    [HttpPost]
    [ProducesResponseType(StatusCodes.Status202Accepted)]
    [ProducesResponseType(StatusCodes.Status400BadRequest)]
    [ProducesResponseType(StatusCodes.Status409Conflict)]
    public async Task<IActionResult> Create(
        [FromBody] CreateOrderRequest req,
        [FromHeader(Name = "Idempotency-Key")] string? idempotencyKey,
        CancellationToken ct)
    {
        if (req.Quantity <= 0)
            return BadRequest("Quantity must be positive.");

        if (req.ProductId == Guid.Empty)
            return BadRequest("ProductId must be a non-empty GUID.");

        if (string.IsNullOrWhiteSpace(idempotencyKey))
            return BadRequest("The Idempotency-Key header is required.");

        idempotencyKey = idempotencyKey.Trim();
        if (idempotencyKey.Length > 128 || idempotencyKey.Any(char.IsControl))
            return BadRequest("Idempotency-Key must be 1-128 characters and contain no control characters.");

        var requestHash = ComputeRequestHash(req);

        // Fast path: a previously committed request can be replayed immediately.
        var existing = await FindIdempotencyRecordAsync(idempotencyKey, ct);
        if (existing is not null)
            return ReplayOrConflict(existing, requestHash);

        var orderId = Guid.NewGuid();
        var eventId = Guid.NewGuid();
        var now = DateTime.UtcNow;

        var evt = new OrderCreated(eventId, orderId, req.ProductId, req.Quantity, now);
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

        var idempotencyRecord = new ApiIdempotencyRecord
        {
            Operation = CreateOrderOperation,
            Key = idempotencyKey,
            RequestHash = requestHash,
            ResourceId = orderId,
            ResponseStatusCode = StatusCodes.Status202Accepted,
            CreatedAtUtc = now
        };

        // The key, order and Outbox event commit together. A rollback leaves none of them behind.
        await using var tx = await _db.Database.BeginTransactionAsync(ct);
        _db.ApiIdempotencyRecords.Add(idempotencyRecord);
        _db.Orders.Add(order);
        _db.OutboxMessages.Add(outboxMessage);

        try
        {
            await _db.SaveChangesAsync(ct);
            await tx.CommitAsync(ct);
        }
        catch (DbUpdateException ex) when (IsUniqueConstraintViolation(ex))
        {
            await tx.RollbackAsync(ct);
            _db.ChangeTracker.Clear();

            // Another concurrent request with this key won the unique-index race.
            var concurrentRecord = await FindIdempotencyRecordAsync(idempotencyKey, ct);
            if (concurrentRecord is null)
                throw;

            return ReplayOrConflict(concurrentRecord, requestHash);
        }

        return Accepted($"/api/orders/{orderId}", new
        {
            orderId,
            status = "Submitted"
        });
    }

    private async Task<ApiIdempotencyRecord?> FindIdempotencyRecordAsync(string key, CancellationToken ct)
        => await _db.ApiIdempotencyRecords
            .AsNoTracking()
            .SingleOrDefaultAsync(x => x.Operation == CreateOrderOperation && x.Key == key, ct);

    private IActionResult ReplayOrConflict(ApiIdempotencyRecord record, string requestHash)
    {
        if (!string.Equals(record.RequestHash, requestHash, StringComparison.Ordinal))
            return Conflict(new ProblemDetails
            {
                Status = StatusCodes.Status409Conflict,
                Title = "Idempotency key conflict",
                Detail = "This Idempotency-Key has already been used with a different request payload. Use a new key for a different order."
            });

        Response.Headers["Idempotency-Replayed"] = "true";
        Response.Headers.Location = $"/api/orders/{record.ResourceId}";
        return StatusCode(record.ResponseStatusCode, new
        {
            orderId = record.ResourceId,
            status = "Submitted"
        });
    }

    private static string ComputeRequestHash(CreateOrderRequest request)
    {
        // Explicit canonical representation avoids depending on JSON property ordering/options.
        var canonical = $"{request.ProductId:D}|{request.Quantity}";
        return Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(canonical)));
    }

    private static bool IsUniqueConstraintViolation(DbUpdateException exception)
    {
        for (Exception? current = exception; current is not null; current = current.InnerException)
        {
            if (current is SqlException sqlException &&
                (sqlException.Number == 2601 || sqlException.Number == 2627))
                return true;
        }

        return false;
    }
}
