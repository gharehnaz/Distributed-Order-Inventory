using System.Text.Json;
using Contracts;
using Confluent.Kafka;
using InventoryService.Data;
using InventoryService.Models;
using Microsoft.EntityFrameworkCore;

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
		using var scope = _scopeFactory.CreateScope();

		var db = scope.ServiceProvider
			.GetRequiredService<InventoryDbContext>();

		await using var tx =
			await db.Database.BeginTransactionAsync(ct);

		// 1. بررسی Idempotency
		var alreadyProcessed = await db.ProcessedMessages
			.AnyAsync(x => x.EventId == message.EventId, ct);

		if (alreadyProcessed)
		{
			await tx.RollbackAsync(ct);
			return;
		}

		db.ProcessedMessages.Add(new ProcessedMessage
		{
			EventId = message.EventId,
			ProcessedAtUtc = DateTime.UtcNow
		});
		
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

		await tx.CommitAsync(ct);
	}


}
