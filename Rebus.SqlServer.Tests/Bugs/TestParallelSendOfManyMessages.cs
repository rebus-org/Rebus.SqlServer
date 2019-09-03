using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using NUnit.Framework;
using Rebus.Messages;
using Rebus.SqlServer.Transport;
using Rebus.Tests.Contracts;
using Rebus.Tests.Contracts.Utilities;
using Rebus.Threading.TaskParallelLibrary;
using Rebus.Transport;

namespace Rebus.SqlServer.Tests.Bugs
{
    [TestFixture]
    [Description("An attempt to reproduce an InvalidOperationException coming from using a closed connection somehow...")]
    public class TestParallelSendOfManyMessages : FixtureBase
    {
        [Test]
        [Repeat(100)]
        public async Task SeeIfWeCanProvokeAnInvalidOperationException()
        {
            var queueName = TestConfig.GetName("test-queue");

            SqlTestHelper.DropTable(queueName);

            Using(new DisposableCallback(() => SqlTestHelper.DropTable(queueName)));

            var loggerFactory = new ListLoggerFactory();
            var connectionProvider = new DbConnectionProvider(SqlTestHelper.ConnectionString, loggerFactory);
            var asyncTaskFactory = new TplAsyncTaskFactory(loggerFactory);
            var transport = new SqlServerTransport(connectionProvider, queueName, loggerFactory, asyncTaskFactory);

            Using(transport);

            transport.CreateQueue(queueName);

            using (var scope = new RebusTransactionScope())
            {
                var transactionContext = scope.TransactionContext;

                await Task.WhenAll(Enumerable.Range(0, 1000).Select(n =>
                {
                    var headers = new Dictionary<string, string>
                    {
                        {Headers.MessageId, Guid.NewGuid().ToString() }
                    };
                    var transportMessage = new TransportMessage(headers, new byte[] { 1, 2, 3, 45 });
                    return transport.Send(queueName, transportMessage, transactionContext);
                }));

                await scope.CompleteAsync();
            }
        }
    }
}