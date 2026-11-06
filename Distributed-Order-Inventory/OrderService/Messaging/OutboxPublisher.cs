using Confluent.Kafka;
using Microsoft.EntityFrameworkCore;
using OrderService.Data;
namespace OrderService.Messaging;

public sealed class OutboxPublisher(IServiceScopeFactory scopes, IConfiguration config, ILogger<OutboxPublisher> log) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var producer = new ProducerBuilder<string, string>(new ProducerConfig
        {
            BootstrapServers = config["Kafka:BootstrapServers"] ?? "localhost:9092",
            EnableIdempotence = true,
            Acks = Acks.All
        }).Build();

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                using var scope = scopes.CreateScope();
                var db = scope.ServiceProvider.GetRequiredService<OrderDbContext>();
                var batch = await db.OutboxMessages.Where(x => x.PublishedAtUtc == null)
                    .OrderBy(x => x.OccurredAtUtc).Take(20).ToListAsync(stoppingToken);

                foreach (var msg in batch)
                {
                    try
                    {
                        await producer.ProduceAsync(config["Kafka:Topic"] ?? "order.created",
                            new Message<string, string> { Key = msg.Id.ToString(), Value = msg.Payload }, stoppingToken);
                        msg.PublishedAtUtc = DateTime.UtcNow;
                        msg.LastError = null;
                    }
                    catch (Exception ex)
                    {
                        msg.RetryCount++;
                        msg.LastError = ex.Message.Length > 2000 ? ex.Message.Substring(0, 2000) : ex.Message;
                        log.LogError(ex, "Publishing Outbox message {Id} failed", msg.Id);
                        break;
                    }
                    finally { await db.SaveChangesAsync(stoppingToken); }
                }
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) { break; }
            catch (Exception ex) { log.LogError(ex, "Outbox polling failed"); }
            await Task.Delay(TimeSpan.FromSeconds(2), stoppingToken);
        }
    }
}
