using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using Rebus.Config;
using Rebus.Extensions;
using Rebus.Logging;
using Rebus.Messages;
using Rebus.SqlServer.Transport;
using Rebus.Tests.Contracts;
using Rebus.Threading.TaskParallelLibrary;
using Rebus.Time;
using Rebus.Transport;

namespace Rebus.SqlServer.Tests.Transport
{
    [TestFixture, Category(Categories.SqlServer)]
    public class TestSqlServerTransport : FixtureBase
    {
        const string QueueName = "input";
        SqlServerTransport _transport;
        CancellationToken _cancellationToken;

        protected override void SetUp()
        {
            SqlTestHelper.DropAllTables();

            var rebusTime = new DefaultRebusTime();
            var consoleLoggerFactory = new ConsoleLoggerFactory(false);
            var connectionProvider = new DbConnectionProvider(SqlTestHelper.ConnectionString, consoleLoggerFactory);
            var asyncTaskFactory = new TplAsyncTaskFactory(consoleLoggerFactory);

            _transport = new SqlServerTransport(connectionProvider, QueueName, consoleLoggerFactory, asyncTaskFactory, rebusTime, new SqlServerTransportOptions(connectionProvider));
            _transport.EnsureTableIsCreated();

            Using(_transport);

            _transport.Initialize();

            _cancellationToken = new CancellationTokenSource().Token;
        }

        [Test]
        public async Task ReceivesSentMessageWhenTransactionIsCommitted()
        {
            using (var scope = new RebusTransactionScope())
            {
                await _transport.Send(QueueName, RecognizableMessage(), scope.TransactionContext);

                await scope.CompleteAsync();
            }

            using (var scope = new RebusTransactionScope())
            {
                var transportMessage = await _transport.Receive(scope.TransactionContext, _cancellationToken);

                await scope.CompleteAsync();

                AssertMessageIsRecognized(transportMessage);
            }
        }

        [Test]
        public async Task DoesNotReceiveSentMessageWhenTransactionIsNotCommitted()
        {
            using (var scope = new RebusTransactionScope())
            {
                await _transport.Send(QueueName, RecognizableMessage(), scope.TransactionContext);

                // deliberately skip this:
                //await context.Complete();
            }

            using (var scope = new RebusTransactionScope())
            {
                var transportMessage = await _transport.Receive(scope.TransactionContext, _cancellationToken);

                Assert.That(transportMessage, Is.Null);
            }
        }

        [TestCase(1000)]
        public async Task LotsOfAsyncStuffGoingDown(int numberOfMessages)
        {
            var receivedMessages = 0L;
            var messageIds = new ConcurrentDictionary<int, int>();

            Console.WriteLine("Sending {0} messages", numberOfMessages);

            await Task.WhenAll(Enumerable.Range(0, numberOfMessages)
                .Select(async i =>
                {
                    using var scope = new RebusTransactionScope();
                    await _transport.Send(QueueName, RecognizableMessage(i), scope.TransactionContext);
                    await scope.CompleteAsync();
                    messageIds[i] = 0;
                }));

            Console.WriteLine("Receiving {0} messages", numberOfMessages);

            using (new Timer(_ => Console.WriteLine("Received: {0} msgs", receivedMessages), null, 0, 1000))
            {
                var stopwatch = Stopwatch.StartNew();

                while (Interlocked.Read(ref receivedMessages) < numberOfMessages && stopwatch.Elapsed < TimeSpan.FromMinutes(2))
                {
                    await Task.WhenAll(Enumerable.Range(0, 10).Select(async __ =>
                    {
                        using var scope = new RebusTransactionScope();
                        var msg = await _transport.Receive(scope.TransactionContext, _cancellationToken);
                        await scope.CompleteAsync();

                        if (msg != null)
                        {
                            Interlocked.Increment(ref receivedMessages);
                            var id = int.Parse(msg.Headers["id"]);
                            messageIds.AddOrUpdate(id, 1, (_, existing) => existing + 1);
                        }
                    }));
                }

                await Task.Delay(3000);
            }

            Assert.That(messageIds.Keys.OrderBy(k => k).ToArray(), Is.EqualTo(Enumerable.Range(0, numberOfMessages).ToArray()));

            var kvpsDifferentThanOne = messageIds.Where(kvp => kvp.Value != 1).ToList();

            if (kvpsDifferentThanOne.Any())
            {
                Assert.Fail(@"Oh no! the following IDs were not received exactly once:

{0}",
    string.Join(Environment.NewLine, kvpsDifferentThanOne.Select(kvp => $"   {kvp.Key}: {kvp.Value}")));
            }
        }

        void AssertMessageIsRecognized(TransportMessage transportMessage)
        {
            Assert.That(transportMessage.Headers.GetValue("recognizzle"), Is.EqualTo("hej"));
        }

        static TransportMessage RecognizableMessage(int id = 0)
        {
            var headers = new Dictionary<string, string>
            {
                {"recognizzle", "hej"},
                {"id", id.ToString()}
            };
            return new TransportMessage(headers, new byte[] { 1, 2, 3 });
        }
    }
}
