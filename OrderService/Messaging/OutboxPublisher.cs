using System.Data;
using Confluent.Kafka;
using Microsoft.EntityFrameworkCore;
using OrderService.Data;
using OrderService.Models;

namespace OrderService.Messaging;

public sealed class OutboxPublisher(IServiceScopeFactory scopes,IConfiguration config,ILogger<OutboxPublisher> log) : BackgroundService
{
    private const int BatchSize = 1; // One-at-a-time keeps the 2-minute lease longer than the 60-second Kafka timeout.
    private const int MaxRetryCount = 10;
    private static readonly TimeSpan LeaseDuration = TimeSpan.FromMinutes(2);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var producerConfig = new ProducerConfig
        {
            BootstrapServers = config["Kafka:BootstrapServers"] ?? "localhost:9092",
            EnableIdempotence = true,
            Acks = Acks.All,
            MessageTimeoutMs = 60_000
        };

        using var producer = new ProducerBuilder<string, string>(producerConfig).Build();

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await using var scope = scopes.CreateAsyncScope();
                var db = scope.ServiceProvider.GetRequiredService<OrderDbContext>();

                var batch = await ClaimMessagesAsync(db, BatchSize, stoppingToken);

                foreach (var message in batch)
                {
                    stoppingToken.ThrowIfCancellationRequested();
                    var claimToken = message.LockedBy!;

                    try
                    {
                        await producer.ProduceAsync(
                            config["Kafka:Topic"] ?? "order.created",
                            new Message<string, string>
                            {
                                Key = message.Id.ToString(),
                                Value = message.Payload
                            },
                            stoppingToken);

                        var now = DateTime.UtcNow;
                        var updated = await db.OutboxMessages
                            .Where(x => x.Id == message.Id &&
                                        x.LockedBy == claimToken &&
                                        x.PublishedAtUtc == null)
                            .ExecuteUpdateAsync(setters => setters
                                .SetProperty(x => x.PublishedAtUtc, now)
                                .SetProperty(x => x.LockedBy, (string?)null)
                                .SetProperty(x => x.LockedUntilUtc, (DateTime?)null)
                                .SetProperty(x => x.LastError, (string?)null), stoppingToken);

                        if (updated == 0)
                            log.LogWarning("Outbox message {MessageId} was published but claim ownership was lost before marking it published", message.Id);
                    }
                    catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                    {
                        throw;
                    }
                    catch (Exception ex)
                    {
                        log.LogError(ex, "Publishing Outbox message {MessageId} failed", message.Id);
                        await RecordFailureAsync(db, message, claimToken, ex, stoppingToken);
                    }
                }
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                log.LogError(ex, "Outbox polling failed");
            }

            try { await Task.Delay(TimeSpan.FromSeconds(2), stoppingToken); }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) { break; }
        }
    }

    private static async Task<List<OutboxMessage>> ClaimMessagesAsync(
        OrderDbContext db, int batchSize, CancellationToken ct)
    {
        // A new token per claim batch fences off stale work from an earlier lease.
        var claimToken = Guid.NewGuid().ToString("N");
        var now = DateTime.UtcNow;
        var leaseUntil = now.Add(LeaseDuration);
        var claimedIds = new List<Guid>();

        // Execute the UPDATE ... OUTPUT as a direct DbCommand. This avoids EF
        // wrapping a data-modifying statement as a subquery for SqlQuery<T>.
        var connection = db.Database.GetDbConnection();
        var openedHere = connection.State != ConnectionState.Open;
        if (openedHere)
            await db.Database.OpenConnectionAsync(ct);

        try
        {
            await using var command = connection.CreateCommand();
            command.CommandText = @"
                ;WITH [MessagesToClaim] AS
                (
                    SELECT TOP (@batchSize) [Id]
                    FROM [dbo].[OutboxMessages] WITH (UPDLOCK, READPAST, ROWLOCK)
                    WHERE [PublishedAtUtc] IS NULL
                      AND [IsDeadLetter] = 0
                      AND ([LockedUntilUtc] IS NULL OR [LockedUntilUtc] < @now)
                      AND ([NextRetryAtUtc] IS NULL OR [NextRetryAtUtc] <= @now)
                    ORDER BY [OccurredAtUtc], [Id]
                )
                UPDATE [MessagesToClaim]
                   SET [LockedBy] = @claimToken, [LockedUntilUtc] = @leaseUntil
                OUTPUT INSERTED.[Id];";

            var batchParameter = command.CreateParameter();
            batchParameter.ParameterName = "@batchSize";
            batchParameter.DbType = DbType.Int32;
            batchParameter.Value = batchSize;
            command.Parameters.Add(batchParameter);

            var nowParameter = command.CreateParameter();
            nowParameter.ParameterName = "@now";
            nowParameter.DbType = DbType.DateTime2;
            nowParameter.Value = now;
            command.Parameters.Add(nowParameter);

            var leaseParameter = command.CreateParameter();
            leaseParameter.ParameterName = "@leaseUntil";
            leaseParameter.DbType = DbType.DateTime2;
            leaseParameter.Value = leaseUntil;
            command.Parameters.Add(leaseParameter);

            var tokenParameter = command.CreateParameter();
            tokenParameter.ParameterName = "@claimToken";
            tokenParameter.DbType = DbType.String;
            tokenParameter.Size = 100;
            tokenParameter.Value = claimToken;
            command.Parameters.Add(tokenParameter);

            await using var reader = await command.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
                claimedIds.Add(reader.GetGuid(0));
        }
        finally
        {
            if (openedHere)
                await db.Database.CloseConnectionAsync();
        }

        if (claimedIds.Count == 0)
            return [];

        return await db.OutboxMessages
            .AsNoTracking()
            .Where(x => claimedIds.Contains(x.Id) && x.LockedBy == claimToken)
            .OrderBy(x => x.OccurredAtUtc)
            .ToListAsync(ct);
    }

    private static async Task RecordFailureAsync(
        OrderDbContext db, OutboxMessage message, string claimToken,
        Exception exception, CancellationToken ct)
    {
        var retryCount = message.RetryCount + 1;
        var now = DateTime.UtcNow;
        var error = exception.ToString();
        if (error.Length > 2000) error = error[..2000];

        var deadLetter = retryCount >= MaxRetryCount;
        var delaySeconds = Math.Min(Math.Pow(2, retryCount), 300);
        DateTime? nextRetry = deadLetter ? null : now.AddSeconds(delaySeconds);

        await db.OutboxMessages
            .Where(x => x.Id == message.Id && x.LockedBy == claimToken && x.PublishedAtUtc == null)
            .ExecuteUpdateAsync(setters => setters
                .SetProperty(x => x.RetryCount, retryCount)
                .SetProperty(x => x.LastError, error)
                .SetProperty(x => x.NextRetryAtUtc, nextRetry)
                .SetProperty(x => x.IsDeadLetter, deadLetter)
                .SetProperty(x => x.DeadLetterAtUtc, deadLetter ? now : (DateTime?)null)
                .SetProperty(x => x.LockedBy, (string?)null)
                .SetProperty(x => x.LockedUntilUtc, (DateTime?)null), ct);
    }
}
