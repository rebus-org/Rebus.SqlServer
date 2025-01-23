﻿using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using NUnit.Framework;
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

    protected override void SetUp()
    {
        base.SetUp();

        const string tableName = "Outbox";

        SqlTestHelper.DropAllTables();

        _storage = new SqlServerOutboxStorage(GetNewDbConnection, new TableName("dbo", tableName));
        _storage.Initialize();
    }

    [Test]
    public async Task CanStoreBatchOfMessages_Roundtrip()
    {
        var transportMessage = new TransportMessage(new Dictionary<string, string>(), new byte[] { 1, 2, 3 });
        var outgoingMessage = new OutgoingTransportMessage(transportMessage, "wherever");

        await _storage.Save(new[] { outgoingMessage });

        using var outboxMessageBatch = await _storage.GetNextMessageBatch();

        Assert.That(outboxMessageBatch.Count(), Is.EqualTo(1));
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
            var outgoingMessage = new OutgoingTransportMessage(transportMessage, "wherever");

            await _storage.Save(new[] { outgoingMessage }, dbConnection);

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
        var outgoingMessage = new OutgoingTransportMessage(transportMessage, "wherever");

        await _storage.Save(new[] { outgoingMessage });

        using var batch1 = await _storage.GetNextMessageBatch();
        await batch1.Complete();

        using var batch2 = await _storage.GetNextMessageBatch();

        Assert.That(batch1.Count(), Is.EqualTo(1));
        Assert.That(batch2.Count(), Is.EqualTo(0));
    }
    
    [Test]
    public async Task CanStoreLargeBatchOfMessages_Complete()
    {
        var transportMessage = new TransportMessage(new Dictionary<string, string>(), new byte[] { 1, 2, 3 });
        var outgoingMessage = new OutgoingTransportMessage(transportMessage, "wherever");
        const int messageCount = 1100;
        var outgoingMessages = Enumerable.Repeat(outgoingMessage, messageCount).ToArray();

        await _storage.Save(outgoingMessages);

        using var batch1 = await _storage.GetNextMessageBatch(maxMessageBatchSize: messageCount);
        await batch1.Complete();

        using var batch2 = await _storage.GetNextMessageBatch();

        Assert.That(batch1.Count(), Is.EqualTo(messageCount));
        Assert.That(batch2.Count(), Is.EqualTo(0));
    }

    [Test]
    public async Task CanGetBatchesOfMessages_VaryingBatchSize()
    {
        static OutgoingTransportMessage CreateOutgoingMessage(string body)
        {
            var transportMessage = new TransportMessage(new Dictionary<string, string>(), Encoding.UTF8.GetBytes(body));
            var outgoingMessage1 = new OutgoingTransportMessage(transportMessage, "wherever");
            return outgoingMessage1;
        }

        var texts = Enumerable.Range(0, 100).Select(n => $"message {n:000}").ToList();
        await _storage.Save(texts.Select(CreateOutgoingMessage));

        using var batch1 = await _storage.GetNextMessageBatch(maxMessageBatchSize: 10);
        Assert.That(batch1.Count(), Is.EqualTo(10));

        using var batch2 = await _storage.GetNextMessageBatch(maxMessageBatchSize: 12);
        Assert.That(batch2.Count(), Is.EqualTo(12));

        using var batch3 = await _storage.GetNextMessageBatch(maxMessageBatchSize: 77);
        Assert.That(batch3.Count(), Is.EqualTo(77));

        using var batch4 = await _storage.GetNextMessageBatch(maxMessageBatchSize: 1);
        Assert.That(batch4.Count(), Is.EqualTo(1));
    }

    [Test]
    public async Task CanGetBatchesOfMessages_TwoBatchesInParallel()
    {
        static OutgoingTransportMessage CreateOutgoingMessage(string body)
        {
            var transportMessage = new TransportMessage(new Dictionary<string, string>(), Encoding.UTF8.GetBytes(body));
            var outgoingMessage1 = new OutgoingTransportMessage(transportMessage, "wherever");
            return outgoingMessage1;
        }

        var texts = Enumerable.Range(0, 200).Select(n => $"message {n:000}").ToList();

        await _storage.Save(texts.Select(CreateOutgoingMessage));

        using var batch1 = await _storage.GetNextMessageBatch();
        Assert.That(batch1.Count(), Is.EqualTo(100));

        using var batch2 = await _storage.GetNextMessageBatch();
        Assert.That(batch2.Count(), Is.EqualTo(100));

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