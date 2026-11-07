using Confluent.Kafka;
using Contracts;
using InventoryService.Data;
using InventoryService.Models;
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using System.Text.Json;

namespace InventoryService.Messaging;

/// <summary>
/// Consumes order.created. Invalid/unreadable messages are moved to a DLQ.
/// Infrastructure/business processing failures are retried in-place and are
/// never skipped, so a later offset cannot commit past a failed message.
/// </summary>
public sealed class OrderCreatedConsumer : BackgroundService
{
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly IConfiguration _configuration;
    private readonly ILogger<OrderCreatedConsumer> _logger;

    public OrderCreatedConsumer(
        IServiceScopeFactory scopeFactory,
        IConfiguration configuration,
        ILogger<OrderCreatedConsumer> logger)
    {
        _scopeFactory = scopeFactory;
        _configuration = configuration;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var bootstrapServers = _configuration["Kafka:BootstrapServers"]
            ?? throw new InvalidOperationException("Kafka:BootstrapServers is not configured.");
        var topic = _configuration["Kafka:Topic"] ?? "order.created";
        var deadLetterTopic = _configuration["Kafka:DeadLetterTopic"] ?? $"{topic}.dlq";

        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = bootstrapServers,
            GroupId = _configuration["Kafka:GroupId"] ?? "inventory-service",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false,
            EnableAutoOffsetStore = false
        };

        var producerConfig = new ProducerConfig
        {
            BootstrapServers = bootstrapServers,
            ClientId = "inventory-service-dlq-producer",
            Acks = Acks.All,
            EnableIdempotence = true
        };

        using var consumer = new ConsumerBuilder<string, string>(consumerConfig).Build();
        using var producer = new ProducerBuilder<string, string>(producerConfig).Build();
        consumer.Subscribe(topic);

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
                    _logger.LogError(ex, "Kafka consume failed for topic {Topic}", topic);
                    await Task.Delay(TimeSpan.FromSeconds(2), stoppingToken);
                    continue;
                }

                // Keep retrying this exact record until it is processed or safely DLQ'd.
                // Do not call Consume for a later record while this one is unresolved.
                var retryAttempt = 0;
                while (!stoppingToken.IsCancellationRequested)
                {
                    try
                    {
                        var message = DeserializeAndValidate(result.Message.Value);
                        await ProcessMessageAsync(message, stoppingToken);
                        consumer.Commit(result);
                        _logger.LogInformation(
                            "Committed OrderCreated {EventId} at {TopicPartitionOffset}",
                            message.EventId, result.TopicPartitionOffset);
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

                        // Commit source offset only after Kafka acknowledges the DLQ publish.
                        try
                        {
                            await PublishToDeadLetterAsync(producer, deadLetterTopic, result, ex, stoppingToken);
                            consumer.Commit(result);
                            _logger.LogWarning(
                                "Moved message from {TopicPartitionOffset} to {DeadLetterTopic}",
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

    private static OrderCreated DeserializeAndValidate(string? payload)
    {
        if (string.IsNullOrWhiteSpace(payload))
            throw new InvalidDataException("OrderCreated payload is empty.");

        OrderCreated? message;
        try
        {
            message = JsonSerializer.Deserialize<OrderCreated>(payload);
        }
        catch (JsonException ex)
        {
            throw new InvalidDataException("OrderCreated payload is not valid JSON or does not match the contract.", ex);
        }

        if (message is null)
            throw new InvalidDataException("OrderCreated payload deserialized to null.");
        if (message.EventId == Guid.Empty || message.OrderId == Guid.Empty || message.ProductId == Guid.Empty)
            throw new InvalidDataException("OrderCreated contains an empty identifier.");
        if (message.Quantity <= 0)
            throw new InvalidDataException("OrderCreated quantity must be greater than zero.");
        if (message.OccurredAtUtc == default)
            throw new InvalidDataException("OrderCreated OccurredAtUtc is missing.");

        return message;
    }

    private static bool IsPoisonMessage(Exception exception) =>
        exception is InvalidDataException or JsonException or NotSupportedException;

    private static TimeSpan GetRetryDelay(int attempt)
    {
        // Exponential backoff, capped at 30 seconds.
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

        // ProduceAsync completes only after broker acknowledgement (Acks=All).
        await producer.ProduceAsync(deadLetterTopic, deadLetterMessage, ct);
    }

    private static string Truncate(string value, int maxLength) =>
        value.Length <= maxLength ? value : value[..maxLength];

    private async Task ProcessMessageAsync(OrderCreated message, CancellationToken ct)
    {
        await using var scope = _scopeFactory.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredService<InventoryDbContext>();
        await using var tx = await db.Database.BeginTransactionAsync(ct);

        try
        {
            db.ProcessedMessages.Add(new ProcessedMessage
            {
                Id = Guid.NewGuid(),
                EventId = message.EventId,
                ProcessedAtUtc = DateTime.UtcNow
            });
            await db.SaveChangesAsync(ct);

            var affectedRows = await db.InventoryItems
                .Where(x => x.ProductId == message.ProductId &&
                            x.AvailableQuantity >= message.Quantity)
                .ExecuteUpdateAsync(setters => setters.SetProperty(
                    x => x.AvailableQuantity,
                    x => x.AvailableQuantity - message.Quantity), ct);

            var now = DateTime.UtcNow;
            object response = affectedRows == 1
                ? new InventoryReserved(Guid.NewGuid(), message.OrderId, message.ProductId, message.Quantity, now)
                : new InventoryRejected(Guid.NewGuid(), message.OrderId, message.ProductId,
                    message.Quantity, "InsufficientInventory", now);

            db.OutboxMessages.Add(new OutboxMessage
            {
                Id = Guid.NewGuid(),
                Type = response.GetType().Name,
                Payload = JsonSerializer.Serialize(response, response.GetType()),
                OccurredAtUtc = now
            });

            await db.SaveChangesAsync(ct);
            await tx.CommitAsync(ct);
        }
        catch (DbUpdateException ex) when (IsUniqueConstraintViolation(ex))
        {
            await tx.RollbackAsync(ct);
            _logger.LogInformation("Duplicate OrderCreated event {EventId} ignored", message.EventId);
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
