using Confluent.Kafka;
using Contracts;
using InventoryService.Data;
using InventoryService.Models;
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using System.Text.Json;

namespace InventoryService.Messaging;

public class OrderCreatedConsumer : BackgroundService
{
	private readonly IServiceScopeFactory _scopeFactory;
	private readonly IConfiguration _configuration;

	public OrderCreatedConsumer(
		IServiceScopeFactory scopeFactory,
		IConfiguration configuration)
	{
		_scopeFactory = scopeFactory;
		_configuration = configuration;
	}

	protected override async Task ExecuteAsync(
		CancellationToken stoppingToken)
	{
		var config = new ConsumerConfig
		{
			BootstrapServers = _configuration["Kafka:BootstrapServers"],
			GroupId = "inventory-service",
			AutoOffsetReset = AutoOffsetReset.Earliest,
			EnableAutoCommit = false
		};

		using var consumer =
			new ConsumerBuilder<string, string>(config).Build();

		consumer.Subscribe("order.created");

		while (!stoppingToken.IsCancellationRequested)
		{
			try
			{
				var result = consumer.Consume(stoppingToken);

				var message = JsonSerializer.Deserialize<OrderCreated>(
					result.Message.Value);

				if (message is null)
					continue;

				await ProcessMessageAsync(message, stoppingToken);

				consumer.Commit(result);
			}
			catch (OperationCanceledException)
			{
				break;
			}
			catch (Exception ex)
			{
				Console.WriteLine(ex.Message);
				await Task.Delay(1000, stoppingToken);
			}
		}

		consumer.Close();
	}

	
	private async Task ProcessMessageAsync(
    OrderCreated message,
    CancellationToken ct)
{
    await using var scope = _scopeFactory.CreateAsyncScope();

    var db = scope.ServiceProvider
        .GetRequiredService<InventoryDbContext>();

    await using var tx =
        await db.Database.BeginTransactionAsync(ct);

    try
    {
        // 1. Inbox: ثبت پیام ورودی
        db.ProcessedMessages.Add(new ProcessedMessage
        {
            Id = Guid.NewGuid(),
            EventId = message.EventId,
            ProcessedAtUtc = DateTime.UtcNow
        });

        // ثبت واقعی در دیتابیس
        await db.SaveChangesAsync(ct);

        // 2. Business Logic: کاهش اتمیک موجودی
        var affectedRows = await db.InventoryItems
            .Where(x =>
                x.ProductId == message.ProductId &&
                x.AvailableQuantity >= message.Quantity)
            .ExecuteUpdateAsync(
                setters => setters.SetProperty(
                    x => x.AvailableQuantity,
                    x => x.AvailableQuantity - message.Quantity),
                ct);

        var now = DateTime.UtcNow;

        object response = affectedRows == 1
            ? new InventoryReserved(
                Guid.NewGuid(),
                message.OrderId,
                message.ProductId,
                message.Quantity,
                now)
            : new InventoryRejected(
                Guid.NewGuid(),
                message.OrderId,
                message.ProductId,
                message.Quantity,
                "InsufficientInventory",
                now);

        // 3. ثبت Outbox
        db.OutboxMessages.Add(new OutboxMessage
        {
            Id = Guid.NewGuid(),
            Type = response.GetType().Name,
            Payload = JsonSerializer.Serialize(
                response,
                response.GetType()),
            OccurredAtUtc = now
        });

        await db.SaveChangesAsync(ct);

        // 4. Commit تمام تغییرات
        await tx.CommitAsync(ct);

			Console.WriteLine(
            "Processed OrderCreated event {EventId}",
            message.EventId);
    }
    catch (DbUpdateException ex)
        when (IsUniqueConstraintViolation(ex))
    {
        await tx.RollbackAsync(ct);

			Console.WriteLine(
				"Ignored duplicate OrderCreated event {EventId}",
            message.EventId);

        // پیام تکراری است؛ Business Logic اجرا نمی‌شود.
    }
    catch
    {
        await tx.RollbackAsync(ct);
        throw;
    }
}
	private static bool IsUniqueConstraintViolation(
		Exception exception)
	{
		for (
			Exception? current = exception;
			current is not null;
			current = current.InnerException)
		{
			if (current is SqlException sqlException &&
				(sqlException.Number == 2601 ||
				 sqlException.Number == 2627))
			{
				return true;
			}
		}

		return false;
	}
}
