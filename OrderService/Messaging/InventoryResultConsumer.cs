
using Confluent.Kafka;
using Contracts;
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using OrderService.Data;
using OrderService.Models;
using System.Text.Json;

namespace OrderService.Messaging
{
	public class InventoryResultConsumer : BackgroundService
	{
		private readonly IServiceScopeFactory _scopeFactory;
		private readonly IConfiguration _configuration;
		private readonly ILogger<InventoryResultConsumer> _logger;

		public InventoryResultConsumer(
			IServiceScopeFactory scopeFactory,
			IConfiguration configuration,
			ILogger<InventoryResultConsumer> logger)
		{
			_scopeFactory = scopeFactory;
			_configuration = configuration;
			_logger = logger;
		}

		protected override async Task ExecuteAsync(
			CancellationToken stoppingToken)
		{
			var config = new ConsumerConfig
			{
				BootstrapServers = _configuration["Kafka:BootstrapServers"],
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

			try
			{
				while (!stoppingToken.IsCancellationRequested)
				{
					ConsumeResult<string, string> result;

					try
					{
						result = consumer.Consume(stoppingToken);
					}
					catch (OperationCanceledException)
						when (stoppingToken.IsCancellationRequested)
					{
						break;
					}

					try
					{
						if (result.Topic == "inventory-reserved")
						{
							var message =
								JsonSerializer.Deserialize<InventoryReserved>(
									result.Message.Value)
								?? throw new InvalidDataException(
									"InventoryReserved payload is empty.");

							await ProcessInventoryResultAsync(
								message.EventId,
								message.OrderId,
								OrderStatus.Confirmed,
								null,
								stoppingToken);
						}
						else if (result.Topic == "inventory-rejected")
						{
							var message =
								JsonSerializer.Deserialize<InventoryRejected>(
									result.Message.Value)
								?? throw new InvalidDataException(
									"InventoryRejected payload is empty.");

							await ProcessInventoryResultAsync(
								message.EventId,
								message.OrderId,
								OrderStatus.Rejected,
								message.Reason,
								stoppingToken);
						}
						else
						{
							throw new InvalidOperationException(
								$"Unexpected topic: {result.Topic}");
						}

						// Commit only after successful processing
						// or confirmed duplicate detection.
						consumer.Commit(result);
					}
					catch (OperationCanceledException)
						when (stoppingToken.IsCancellationRequested)
					{
						break;
					}
					catch (Exception ex)
					{
						_logger.LogError(ex,
							"Failed to process message. Topic: {Topic}, Offset: {Offset}",
							result.Topic,
							result.Offset.Value);

						// Do not commit. Kafka can redeliver this message.
						await Task.Delay(1000, stoppingToken);
					}
				}
			}
			finally
			{
				consumer.Close();
			}
		}

		private async Task ProcessInventoryResultAsync(
			Guid eventId,
			Guid orderId,
			OrderStatus status,
			string? reason,
			CancellationToken ct)
		{
			await using var scope = _scopeFactory.CreateAsyncScope();

			var db = scope.ServiceProvider
				.GetRequiredService<OrderDbContext>();

			await using var tx =
				await db.Database.BeginTransactionAsync(ct);

			try
			{
				// 1. Register EventId in Inbox
				db.ProcessedMessages.Add(new ProcessedMessage
				{
					EventId = eventId,
					ProcessedAtUtc = DateTime.UtcNow
				});

				await db.SaveChangesAsync(ct);

				// 2. Update Order
				var order = await db.Orders
					.FirstOrDefaultAsync(x => x.Id == orderId, ct);

				if (order is not null &&
					order.Status == OrderStatus.Pending)
				{
					order.Status = status;
					order.RejectionReason = reason;

					await db.SaveChangesAsync(ct);
				}

				// 3. Commit both Inbox and Order changes
				await tx.CommitAsync(ct);

				_logger.LogInformation(
					"Processed event {EventId} for order {OrderId}",
					eventId,
					orderId);
			}
			catch (DbUpdateException ex)
				when (IsUniqueConstraintViolation(ex))
			{
				await tx.RollbackAsync(ct);

				_logger.LogInformation(
					"Duplicate event {EventId} ignored",
					eventId);
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
			for (Exception? current = exception;
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
}
