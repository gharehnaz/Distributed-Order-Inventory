using Confluent.Kafka;
using Contracts;
using OrderService.Data;
using OrderService.Models;
using Microsoft.EntityFrameworkCore;

using System.Text.Json;

namespace OrderService.Messaging
{
	public class InventoryResultConsumer : BackgroundService
	{
		private readonly IServiceScopeFactory _scopeFactory;
		private readonly IConfiguration _configuration;

		public InventoryResultConsumer(
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
				BootstrapServers =
					_configuration["Kafka:BootstrapServers"],

				GroupId = "order-service",
				AutoOffsetReset = AutoOffsetReset.Earliest,
				EnableAutoCommit = false
			};

			using var consumer =
				new ConsumerBuilder<string, string>(config).Build();

			consumer.Subscribe(new[]
			{
			"inventory-reserved",
			"inventory-rejected"
		});

			while (!stoppingToken.IsCancellationRequested)
			{
				try
				{
					var result = consumer.Consume(stoppingToken);

					using var scope = _scopeFactory.CreateScope();

					var db = scope.ServiceProvider
						.GetRequiredService<OrderDbContext>();

					await using var tx =
						await db.Database.BeginTransactionAsync(stoppingToken);

					if (result.Topic == "inventory-reserved")
					{
						var message =
							JsonSerializer.Deserialize<InventoryReserved>(
								result.Message.Value);

						if (message is not null)
						{
							await UpdateOrderAsync(
								db,
								message.EventId,
								message.OrderId,
								OrderStatus.Confirmed,
								null,
								stoppingToken);
						}
					}
					else if (result.Topic == "inventory-rejected")
					{
						var message =
							JsonSerializer.Deserialize<InventoryRejected>(
								result.Message.Value);

						if (message is not null)
						{
							await UpdateOrderAsync(
								db,
								message.EventId,
								message.OrderId,
								OrderStatus.Rejected,
								message.Reason,
								stoppingToken);
						}
					}

					await tx.CommitAsync(stoppingToken);

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

		private static async Task UpdateOrderAsync(
	OrderDbContext db,
	Guid eventId,
	Guid orderId,
	OrderStatus status,
	string? reason,
	CancellationToken ct)
		{
			var alreadyProcessed = await db.ProcessedMessages
				.AnyAsync(x => x.EventId == eventId, ct);

			if (alreadyProcessed)
				return;

			var order = await db.Orders
				.FirstOrDefaultAsync(x => x.Id == orderId, ct);

			if (order is not null &&
				order.Status == OrderStatus.Pending)
			{
				order.Status = status;
				order.RejectionReason = reason;
			}

			db.ProcessedMessages.Add(new ProcessedMessage
			{
				EventId = eventId,
				ProcessedAtUtc = DateTime.UtcNow
			});

			await db.SaveChangesAsync(ct);
		}
	}
}
