using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using NUnit.Framework;
using Rebus.Config.Outbox;
using Rebus.Messages;
using Rebus.SqlServer.Outbox;
using Rebus.Tests.Contracts;
using Rebus.Transport;
// ReSharper disable ArgumentsStyleLiteral

namespace Rebus.SqlServer.Tests.Outbox;

[TestFixture]
public class TestSqlServerOutboxStorage : FixtureBase
{
    SqlServerOutboxStorage _storage;
    FakeRebusTime _fakeRebusTime;
    OutboxOptionsBuilder _options;

    protected override void SetUp()
    {
        base.SetUp();

        const string tableName = "Outbox";

        SqlTestHelper.DropAllTables();

        _fakeRebusTime = new FakeRebusTime();
        _options = new OutboxOptionsBuilder();
        _storage = new SqlServerOutboxStorage(GetNewDbConnection, new TableName("dbo", tableName), _options, _fakeRebusTime);
        _storage.Initialize();
    }

    [Test]
    public async Task IdempotencyMarkersAreRemovedWhenTheyExpire()
    {
        _options.SetIdempotencyTimeout(TimeSpan.FromHours(1.5));

        var now = DateTimeOffset.Now;

        _fakeRebusTime.SetNow(now);

        using (var scope = new RebusTransactionScope())
        {
            using var connection = GetNewDbConnection(scope.TransactionContext);
            await _storage.MarkMessageAsProcessed(connection, "queue-1", "message-1");
            await connection.Complete();
            await scope.CompleteAsync();
        }

        _fakeRebusTime.SetNow(now.AddHours(1));

        using (var scope = new RebusTransactionScope())
        {
            using var connection = GetNewDbConnection(scope.TransactionContext);
            await _storage.MarkMessageAsProcessed(connection, "queue-1", "message-2");
            await connection.Complete();
            await scope.CompleteAsync();
        }

        _fakeRebusTime.SetNow(now.AddHours(2));

        // we expect the first marker to have been cleaned up
        await _storage.CleanUp();

        using (var scope = new RebusTransactionScope())
        {
            using var connection = GetNewDbConnection(scope.TransactionContext);

            var message1Processed = await _storage.HasProcessedMessage(connection, "queue-1", "message-1");
            var message2Processed = await _storage.HasProcessedMessage(connection, "queue-1", "message-2");

            Assert.That(message1Processed, Is.False);
            Assert.That(message2Processed, Is.True);
        }
    }

    [Test]
    public async Task CanRememberMessages()
    {
        using (var scope = new RebusTransactionScope())
        {
            using var connection = GetNewDbConnection(scope.TransactionContext);
            await _storage.MarkMessageAsProcessed(connection, "queue-1", "message-1");
            await connection.Complete();
            await scope.CompleteAsync();
        }

        using (var scope = new RebusTransactionScope())
        {
            using var connection = GetNewDbConnection(scope.TransactionContext);
            await _storage.MarkMessageAsProcessed(connection, "queue-1", "message-2");
            await connection.Complete();
            await scope.CompleteAsync();
        }

        using (var scope = new RebusTransactionScope())
        {
            using var connection = GetNewDbConnection(scope.TransactionContext);
            await _storage.MarkMessageAsProcessed(connection, "queue-1", "message-3");
            //await connection.Complete(); //< NO COMPLETE!
            await scope.CompleteAsync();
        }

        using (var scope = new RebusTransactionScope())
        {
            using var connection = GetNewDbConnection(scope.TransactionContext);
            await _storage.MarkMessageAsProcessed(connection, "queue-2", "message-3"); //< ANOTHER QUEUE!
            await connection.Complete();
            await scope.CompleteAsync();
        }

        using (var scope = new RebusTransactionScope())
        {
            using var connection = GetNewDbConnection(scope.TransactionContext);

            var message1Processed = await _storage.HasProcessedMessage(connection, "queue-1", "message-1");
            var message2Processed = await _storage.HasProcessedMessage(connection, "queue-1", "message-2");
            var message3Processed = await _storage.HasProcessedMessage(connection, "queue-1", "message-3");
            var message3ProcessedInAnotherQueue = await _storage.HasProcessedMessage(connection, "queue-2", "message-3");

            Assert.That(message1Processed, Is.True);
            Assert.That(message2Processed, Is.True);
            Assert.That(message3Processed, Is.False);
            Assert.That(message3ProcessedInAnotherQueue, Is.True);
        }
    }

    [Test]
    public async Task CanStoreBatchOfMessages_Roundtrip()
    {
        var transportMessage = new TransportMessage(new Dictionary<string, string>(), new byte[] { 1, 2, 3 });
        var outgoingMessage = new AbstractRebusTransport.OutgoingMessage(transportMessage, "wherever");

        await _storage.Save(new[] { outgoingMessage });

        using var outboxMessageBatch = await _storage.GetNextMessageBatch();

        Assert.That(outboxMessageBatch.Count, Is.EqualTo(1));
        var outboxMessage = outboxMessageBatch.First();
        Assert.That(outboxMessage.DestinationAddress, Is.EqualTo("wherever"));
        Assert.That(outboxMessage.Body, Is.EqualTo(new byte[] { 1, 2, 3 }));
    }

    [TestCase(true)]
    [TestCase(false)]
    public async Task CanStoreBatchOfMessages_ManagedExternally(bool commitAndExpectTheMessagesToBeThere)
    {
        await using (var connection = new SqlConnection(SqlTestHelper.ConnectionString))
        {
            await connection.OpenAsync();

            await using var transaction = connection.BeginTransaction();

            var dbConnection = new DbConnectionWrapper(connection, transaction, managedExternally: true);

            var transportMessage = new TransportMessage(new Dictionary<string, string>(), new byte[] { 1, 2, 3 });
            var outgoingMessage = new AbstractRebusTransport.OutgoingMessage(transportMessage, "wherever");

            await _storage.Save(dbConnection, new[] { outgoingMessage });

            if (commitAndExpectTheMessagesToBeThere)
            {
                await transaction.CommitAsync();
            }
        }

        if (commitAndExpectTheMessagesToBeThere)
        {
            using var batch1 = await _storage.GetNextMessageBatch();
            await batch1.Complete();

            using var batch2 = await _storage.GetNextMessageBatch();

            Assert.That(batch1.Count, Is.EqualTo(1));
            Assert.That(batch2.Count, Is.EqualTo(0));
        }
        else
        {
            using var batch = await _storage.GetNextMessageBatch();

            Assert.That(batch.Count, Is.EqualTo(0));
        }
    }

    [Test]
    public async Task CanStoreBatchOfMessages_Complete()
    {
        var transportMessage = new TransportMessage(new Dictionary<string, string>(), new byte[] { 1, 2, 3 });
        var outgoingMessage = new AbstractRebusTransport.OutgoingMessage(transportMessage, "wherever");

        await _storage.Save(new[] { outgoingMessage });

        using var batch1 = await _storage.GetNextMessageBatch();
        await batch1.Complete();

        using var batch2 = await _storage.GetNextMessageBatch();

        Assert.That(batch1.Count, Is.EqualTo(1));
        Assert.That(batch2.Count, Is.EqualTo(0));
    }

    [Test]
    public async Task CanStoreBatchOfMessages_Complete_MagicMessageMarkerDoesNotInterfere()
    {
        using var scope = new RebusTransactionScope();
        using var connection = GetNewDbConnection(scope.TransactionContext);

        var transportMessage = new TransportMessage(new Dictionary<string, string>(), new byte[] { 1, 2, 3 });
        var outgoingMessage = new AbstractRebusTransport.OutgoingMessage(transportMessage, "wherever");

        await _storage.Save(connection, new[] { outgoingMessage });
        await _storage.MarkMessageAsProcessed(connection, "queue-1", "message-1");

        await connection.Complete();

        using var batch = await _storage.GetNextMessageBatch();

        Assert.That(batch.Count, Is.EqualTo(1));
    }

    [Test]
    public async Task CanGetBatchesOfMessages_VaryingBatchSize()
    {
        static AbstractRebusTransport.OutgoingMessage CreateOutgoingMessage(string body)
        {
            var transportMessage = new TransportMessage(new Dictionary<string, string>(), Encoding.UTF8.GetBytes(body));
            var outgoingMessage1 = new AbstractRebusTransport.OutgoingMessage(transportMessage, "wherever");
            return outgoingMessage1;
        }

        var texts = Enumerable.Range(0, 100).Select(n => $"message {n:000}").ToList();
        await _storage.Save(texts.Select(CreateOutgoingMessage));

        using var batch1 = await _storage.GetNextMessageBatch(maxMessageBatchSize: 10);
        Assert.That(batch1.Count, Is.EqualTo(10));

        using var batch2 = await _storage.GetNextMessageBatch(maxMessageBatchSize: 12);
        Assert.That(batch2.Count, Is.EqualTo(12));

        using var batch3 = await _storage.GetNextMessageBatch(maxMessageBatchSize: 77);
        Assert.That(batch3.Count, Is.EqualTo(77));

        using var batch4 = await _storage.GetNextMessageBatch(maxMessageBatchSize: 1);
        Assert.That(batch4.Count, Is.EqualTo(1));
    }

    [Test]
    public async Task CanGetBatchesOfMessages_TwoBatchesInParallel()
    {
        static AbstractRebusTransport.OutgoingMessage CreateOutgoingMessage(string body)
        {
            var transportMessage = new TransportMessage(new Dictionary<string, string>(), Encoding.UTF8.GetBytes(body));
            var outgoingMessage1 = new AbstractRebusTransport.OutgoingMessage(transportMessage, "wherever");
            return outgoingMessage1;
        }

        var texts = Enumerable.Range(0, 200).Select(n => $"message {n:000}").ToList();

        await _storage.Save(texts.Select(CreateOutgoingMessage));

        using var batch1 = await _storage.GetNextMessageBatch();
        Assert.That(batch1.Count, Is.EqualTo(100));

        using var batch2 = await _storage.GetNextMessageBatch();
        Assert.That(batch2.Count, Is.EqualTo(100));

        var roundtrippedTexts = batch1.Concat(batch2).Select(b => Encoding.UTF8.GetString(b.Body)).ToList();

        Assert.That(roundtrippedTexts.OrderBy(t => t), Is.EqualTo(texts));
    }

    static IDbConnection GetNewDbConnection(ITransactionContext _)
    {
        var connection = new SqlConnection(SqlTestHelper.ConnectionString);
        connection.Open();
        var transaction = connection.BeginTransaction();
        return new DbConnectionWrapper(connection, transaction, managedExternally: false);
    }
}