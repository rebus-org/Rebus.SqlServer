using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using Rebus.Config;
using Rebus.Logging;
using Rebus.Messages;
using Rebus.SqlServer.Transport;
using Rebus.Tests.Contracts;
using Rebus.Threading.TaskParallelLibrary;
using Rebus.Time;
using Rebus.Transport;

namespace Rebus.SqlServer.Tests.Transport
{
    [TestFixture]
    public class TestSqlServerTransportMessageOrdering : FixtureBase
    {
        const string QueueName = "test-ordering";
        protected override void SetUp() => SqlTestHelper.DropAllTables();

        [TestCase(TransportType.LockedRows)]
        [TestCase(TransportType.LeaseBased)]
        public async Task DeliversMessagesByVisibleTimeAndNotBeInsertionTime(TransportType transportType)
        {
            var transport = GetTransport(transportType);

            var now = DateTime.Now;

            await PutInQueue(transport, GetTransportMessage("first message"));
            await PutInQueue(transport, GetTransportMessage("second message", deferredUntilTime: now.AddMinutes(-1)));
            await PutInQueue(transport, GetTransportMessage("third message", deferredUntilTime: now.AddMinutes(-2)));

            var firstMessage = await ReceiveMessageBody(transport);
            var secondMessage = await ReceiveMessageBody(transport);
            var thirdMessage = await ReceiveMessageBody(transport);

            // expect messages to be received in reverse order because of their visible times
            Assert.That(firstMessage, Is.EqualTo("third message"));
            Assert.That(secondMessage, Is.EqualTo("second message"));
            Assert.That(thirdMessage, Is.EqualTo("first message"));
        }

        static async Task<string> ReceiveMessageBody(ITransport transport)
        {
            using var scope = new RebusTransactionScope();
            var transportMessage = await transport.Receive(scope.TransactionContext, CancellationToken.None);

            if (transportMessage == null) return null;

            var body = Encoding.UTF8.GetString(transportMessage.Body);

            await scope.CompleteAsync();

            return body;
        }

        public enum TransportType
        {
            LockedRows,
            LeaseBased,
        }

        static TransportMessage GetTransportMessage(string body, DateTime? deferredUntilTime = null)
        {
            var headers = new Dictionary<string, string>
            {
                {Headers.MessageId, Guid.NewGuid().ToString()}
            };

            if (deferredUntilTime != null)
            {
                headers[Headers.DeferredRecipient] = QueueName;
                headers[Headers.DeferredUntil] = deferredUntilTime.Value.ToString("o");
            }

            return new TransportMessage(headers, Encoding.UTF8.GetBytes(body));
        }

        static async Task PutInQueue(ITransport transport, TransportMessage transportMessage)
        {
            using var scope = new RebusTransactionScope();
            await transport.Send(QueueName, transportMessage, scope.TransactionContext);
            await scope.CompleteAsync();
        }

        static SqlServerTransport GetTransport(TransportType transportType)
        {
            var rebusTime = new DefaultRebusTime();
            var loggerFactory = new ConsoleLoggerFactory(false);
            var connectionProvider = new DbConnectionProvider(SqlTestHelper.ConnectionString, loggerFactory);
            var asyncTaskFactory = new TplAsyncTaskFactory(loggerFactory);

            var transport = transportType == TransportType.LeaseBased
                ? new SqlServerLeaseTransport(
                    connectionProvider,
                    QueueName,
                    loggerFactory,
                    asyncTaskFactory,
                    rebusTime,
                    TimeSpan.FromMinutes(1),
                    TimeSpan.FromSeconds(5),
                    () => "who cares",
                    new SqlServerLeaseTransportOptions(connectionProvider)
                )
                : new SqlServerTransport(
                    connectionProvider,
                    QueueName,
                    loggerFactory,
                    asyncTaskFactory,
                    rebusTime,
                    new SqlServerTransportOptions(connectionProvider)
                );

            transport.EnsureTableIsCreated();
            transport.Initialize();

            return transport;
        }
    }
}
