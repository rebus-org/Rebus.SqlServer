using System.Collections.Generic;
using System.Data;
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

namespace Rebus.SqlServer.Tests.Bugs
{
    [TestFixture]
    public class TestConcurrencyAndSnapshotIsolation : FixtureBase
    {
        string _queueName;

        protected override void SetUp()
        {
            SqlTestHelper.Execute($"alter database [{SqlTestHelper.DatabaseName}] set allow_snapshot_isolation on");

            Using(new DisposableCallback(() => SqlTestHelper.Execute($"alter database [{SqlTestHelper.DatabaseName}] set allow_snapshot_isolation off")));

            _queueName = TestConfig.GetName("test-queue");

            SqlTestHelper.DropTable(_queueName);

            var transport = GetTransport(SqlTestHelper.ConnectionString, IsolationLevel.ReadCommitted);
            
            Using(transport);
         
            transport.EnsureTableIsCreated();
        }

        [Test]
        public async Task CannotReceiveSameMessageTwiceEvenWhenSnapahotIsolationIsOn()
        {
            const string sentText = "HEJ MED DIG MIN VEN!!!!!11";
            var transportMessage = new TransportMessage(new Dictionary<string, string>(), Encoding.UTF8.GetBytes(sentText));

            var transport1 = GetTransport(";MultipleActiveResultSets=True;" + SqlTestHelper.ConnectionString, IsolationLevel.Snapshot);
            var transport2 = GetTransport(SqlTestHelper.ConnectionString, IsolationLevel.Snapshot);

            using (var scope = new RebusTransactionScope())
            {
                await transport1.Send(_queueName, transportMessage, scope.TransactionContext);
                await scope.CompleteAsync();
            }

            using var scope1 = new RebusTransactionScope();
            using var scope2 = new RebusTransactionScope();
            var message1 = await transport1.Receive(scope1.TransactionContext, CancellationToken.None);
            var message2 = await transport2.Receive(scope2.TransactionContext, CancellationToken.None);

            Assert.That(message1, Is.Not.Null);
            Assert.That(Encoding.UTF8.GetString(message1.Body), Is.EqualTo(sentText));

            Assert.That(message2, Is.Null,
                "Expected the second message to be null, because we should NOT be able to accidentally peek at the same message as another ongoing transaction");

            await scope2.CompleteAsync();
            await scope1.CompleteAsync();
        }

        SqlServerTransport GetTransport(string connectionString, IsolationLevel isolationLevel)
        {
            var rebusTime = new DefaultRebusTime();
            var rebusLoggerFactory = new ConsoleLoggerFactory(false);

            var connectionProvider = new DbConnectionProvider(connectionString, rebusLoggerFactory)
            {
                IsolationLevel = isolationLevel
            };

            var taskFactory = new TplAsyncTaskFactory(rebusLoggerFactory);

            var transport = new SqlServerTransport(
                connectionProvider: connectionProvider,
                inputQueueName: _queueName,
                rebusLoggerFactory: rebusLoggerFactory,
                asyncTaskFactory: taskFactory,
                rebusTime: rebusTime,
                options: new SqlServerTransportOptions(connectionProvider)
            );

            transport.Initialize();

            return transport;
        }
    }
}
