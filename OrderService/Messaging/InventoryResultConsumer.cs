using Confluent.Kafka;
using Contracts;
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using OrderService.Data;
using OrderService.Models;
using System.Text.Json;

namespace OrderService.Messaging;

/// <summary>
/// Consumes inventory result events. Invalid payloads are sent to a DLQ;
/// transient database failures are retried on the same record without advancing offsets.
/// </summary>
public sealed class InventoryResultConsumer : BackgroundService
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

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var bootstrapServers = _configuration["Kafka:BootstrapServers"]
            ?? throw new InvalidOperationException("Kafka:BootstrapServers is not configured.");
        var reservedTopic = _configuration["Kafka:ReservedTopic"] ?? "inventory-reserved";
        var rejectedTopic = _configuration["Kafka:RejectedTopic"] ?? "inventory-rejected";
        var deadLetterTopic = _configuration["Kafka:DeadLetterTopic"] ?? "inventory.results.dlq";

        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = bootstrapServers,
            GroupId = _configuration["Kafka:GroupId"] ?? "order-service",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false,
            EnableAutoOffsetStore = false
        };
        var producerConfig = new ProducerConfig
        {
            BootstrapServers = bootstrapServers,
            ClientId = "order-service-dlq-producer",
            Acks = Acks.All,
            EnableIdempotence = true
        };

        using var consumer = new ConsumerBuilder<string, string>(consumerConfig).Build();
        using var producer = new ProducerBuilder<string, string>(producerConfig).Build();
        consumer.Subscribe(new[] { reservedTopic, rejectedTopic });

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                ConsumeResult<string, string> result;
                try
                {
                    result = consumer.Consume(stoppingToken);
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    break;
                }
                catch (ConsumeException ex)
                {
                    _logger.LogError(ex, "Kafka consume failed");
                    await Task.Delay(TimeSpan.FromSeconds(2), stoppingToken);
                    continue;
                }

                var retryAttempt = 0;
                while (!stoppingToken.IsCancellationRequested)
                {
                    try
                    {
                        if (result.Topic == reservedTopic)
                        {
                            var message = DeserializeAndValidate<InventoryReserved>(result.Message.Value);
                            await ProcessInventoryResultAsync(message.EventId, message.OrderId,
                                OrderStatus.Confirmed, null, stoppingToken);
                        }
                        else if (result.Topic == rejectedTopic)
                        {
                            var message = DeserializeAndValidate<InventoryRejected>(result.Message.Value);
                            await ProcessInventoryResultAsync(message.EventId, message.OrderId,
                                OrderStatus.Rejected, message.Reason, stoppingToken);
                        }
                        else
                        {
                            throw new InvalidDataException($"Unexpected topic: {result.Topic}");
                        }

                        consumer.Commit(result);
                        _logger.LogInformation("Committed inventory result at {TopicPartitionOffset}",
                            result.TopicPartitionOffset);
                        break;
                    }
                    catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                    {
                        return;
                    }
                    catch (Exception ex) when (IsPoisonMessage(ex))
                    {
                        _logger.LogWarning(ex,
                            "Poison message detected at {TopicPartitionOffset}; publishing to {DeadLetterTopic}",
                            result.TopicPartitionOffset, deadLetterTopic);
                        try
                        {
                            await PublishToDeadLetterAsync(producer, deadLetterTopic, result, ex, stoppingToken);
                            consumer.Commit(result);
                            _logger.LogWarning("Moved message from {TopicPartitionOffset} to {DeadLetterTopic}",
                                result.TopicPartitionOffset, deadLetterTopic);
                            break;
                        }
                        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                        {
                            return;
                        }
                        catch (Exception publishException)
                        {
                            retryAttempt++;
                            var delay = GetRetryDelay(retryAttempt);
                            _logger.LogError(publishException,
                                "Could not publish/commit DLQ record {TopicPartitionOffset}. Retry {RetryAttempt} in {Delay}; source offset remains uncommitted",
                                result.TopicPartitionOffset, retryAttempt, delay);
                            await Task.Delay(delay, stoppingToken);
                        }
                    }
                    catch (Exception ex)
                    {
                        retryAttempt++;
                        var delay = GetRetryDelay(retryAttempt);
                        _logger.LogError(ex,
                            "Transient processing failure for {TopicPartitionOffset}. Retry {RetryAttempt} in {Delay}",
                            result.TopicPartitionOffset, retryAttempt, delay);
                        await Task.Delay(delay, stoppingToken);
                    }
                }
            }
        }
        finally
        {
            consumer.Close();
        }
    }

    private static T DeserializeAndValidate<T>(string? payload) where T : class
    {
        if (string.IsNullOrWhiteSpace(payload))
            throw new InvalidDataException("Inventory result payload is empty.");

        T? message;
        try
        {
            message = JsonSerializer.Deserialize<T>(payload);
        }
        catch (JsonException ex)
        {
            throw new InvalidDataException($"Payload does not match {typeof(T).Name} contract.", ex);
        }

        if (message is null)
            throw new InvalidDataException($"Payload deserialized to null for {typeof(T).Name}.");

        switch (message)
        {
            case InventoryReserved reserved when
                reserved.EventId == Guid.Empty || reserved.OrderId == Guid.Empty ||
                reserved.ProductId == Guid.Empty || reserved.Quantity <= 0 || reserved.OccurredAtUtc == default:
                throw new InvalidDataException("InventoryReserved contains invalid required fields.");
            case InventoryRejected rejected when
                rejected.EventId == Guid.Empty || rejected.OrderId == Guid.Empty ||
                rejected.ProductId == Guid.Empty || rejected.Quantity <= 0 ||
                rejected.OccurredAtUtc == default || string.IsNullOrWhiteSpace(rejected.Reason):
                throw new InvalidDataException("InventoryRejected contains invalid required fields.");
        }
        return message;
    }

    private static bool IsPoisonMessage(Exception exception) =>
        exception is InvalidDataException or JsonException or NotSupportedException;

    private static TimeSpan GetRetryDelay(int attempt)
    {
        var seconds = Math.Min(30, Math.Pow(2, Math.Min(attempt - 1, 5)));
        return TimeSpan.FromSeconds(seconds);
    }

    private static async Task PublishToDeadLetterAsync(
        IProducer<string, string> producer,
        string deadLetterTopic,
        ConsumeResult<string, string> source,
        Exception exception,
        CancellationToken ct)
    {
        var headers = new Headers();
        if (source.Message.Headers is not null)
        {
            foreach (var header in source.Message.Headers)
                headers.Add(header.Key, header.GetValueBytes());
        }

        var deadLetterId = $"{source.Topic}:{source.Partition.Value}:{source.Offset.Value}";
        headers.Add("x-dead-letter-id", System.Text.Encoding.UTF8.GetBytes(deadLetterId));
        headers.Add("x-original-topic", System.Text.Encoding.UTF8.GetBytes(source.Topic));
        headers.Add("x-original-partition", System.Text.Encoding.UTF8.GetBytes(source.Partition.Value.ToString()));
        headers.Add("x-original-offset", System.Text.Encoding.UTF8.GetBytes(source.Offset.Value.ToString()));
        headers.Add("x-dead-letter-reason", System.Text.Encoding.UTF8.GetBytes(Truncate(exception.Message, 2000)));
        headers.Add("x-dead-letter-exception", System.Text.Encoding.UTF8.GetBytes(exception.GetType().FullName ?? exception.GetType().Name));
        headers.Add("x-dead-lettered-at-utc", System.Text.Encoding.UTF8.GetBytes(DateTimeOffset.UtcNow.ToString("O")));

        var deadLetterMessage = new Message<string, string>
        {
            Key = source.Message.Key,
            Value = source.Message.Value,
            Headers = headers,
            Timestamp = source.Message.Timestamp
        };
        await producer.ProduceAsync(deadLetterTopic, deadLetterMessage, ct);
    }

    private static string Truncate(string value, int maxLength) =>
        value.Length <= maxLength ? value : value[..maxLength];

    private async Task ProcessInventoryResultAsync(
        Guid eventId,
        Guid orderId,
        OrderStatus status,
        string? reason,
        CancellationToken ct)
    {
        await using var scope = _scopeFactory.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredService<OrderDbContext>();
        await using var tx = await db.Database.BeginTransactionAsync(ct);

        try
        {
            db.ProcessedMessages.Add(new ProcessedMessage
            {
                EventId = eventId,
                ProcessedAtUtc = DateTime.UtcNow
            });
            await db.SaveChangesAsync(ct);

            var order = await db.Orders.FirstOrDefaultAsync(x => x.Id == orderId, ct);
            if (order is not null && order.Status == OrderStatus.Pending)
            {
                order.Status = status;
                order.RejectionReason = reason;
                await db.SaveChangesAsync(ct);
            }

            await tx.CommitAsync(ct);
            _logger.LogInformation("Processed event {EventId} for order {OrderId}", eventId, orderId);
        }
        catch (DbUpdateException ex) when (IsUniqueConstraintViolation(ex))
        {
            await tx.RollbackAsync(ct);
            _logger.LogInformation("Duplicate event {EventId} ignored", eventId);
        }
        catch
        {
            await tx.RollbackAsync(ct);
            throw;
        }
    }

    private static bool IsUniqueConstraintViolation(Exception exception)
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
