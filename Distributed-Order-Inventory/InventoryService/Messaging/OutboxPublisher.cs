using Confluent.Kafka;
using InventoryService.Data;
using Microsoft.EntityFrameworkCore;

namespace InventoryService.Messaging;

public class OutboxPublisher : BackgroundService
{
	private readonly IServiceScopeFactory _scopeFactory;
	private readonly IConfiguration _configuration;

	public OutboxPublisher(
		IServiceScopeFactory scopeFactory,
		IConfiguration configuration)
	{
		_scopeFactory = scopeFactory;
		_configuration = configuration;
	}

	protected override async Task ExecuteAsync(
		CancellationToken stoppingToken)
	{
		var producerConfig = new ProducerConfig
		{
			BootstrapServers =
				_configuration["Kafka:BootstrapServers"],

			EnableIdempotence = true,
			Acks = Acks.All
		};

		using var producer =
			new ProducerBuilder<string, string>(producerConfig)
				.Build();

		while (!stoppingToken.IsCancellationRequested)
		{
			try
			{
				using var scope = _scopeFactory.CreateScope();

				var db = scope.ServiceProvider
					.GetRequiredService<InventoryDbContext>();

				var messages = await db.OutboxMessages
					.Where(x => x.ProcessedAtUtc == null)
					.OrderBy(x => x.OccurredAtUtc)
					.Take(50)
					.ToListAsync(stoppingToken);

				foreach (var message in messages)
				{
					try
					{
						var topic = message.Type switch
						{
							"InventoryReserved" => "inventory-reserved",
							"InventoryRejected" => "inventory-rejected",
							_ => throw new InvalidOperationException(
								$"Unknown event type: {message.Type}")
						};

						await producer.ProduceAsync(
							topic,
							new Message<string, string>
							{
								Key = message.Id.ToString(),
								Value = message.Payload
							},
							stoppingToken);

						message.ProcessedAtUtc = DateTime.UtcNow;
						message.LastError = null;

						await db.SaveChangesAsync(stoppingToken);
					}
					catch (Exception ex)
					{
						message.LastError = ex.Message.Length > 2000
							? ex.Message.Substring(0, 2000)
							: ex.Message;

						await db.SaveChangesAsync(stoppingToken);
					}
				}
			}
			catch (OperationCanceledException)
			{
				break;
			}
			catch (Exception ex)
			{
				Console.WriteLine(ex.Message);
			}

			await Task.Delay(1000, stoppingToken);
		}
	}
}