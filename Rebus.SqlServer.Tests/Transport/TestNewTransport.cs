using System.Linq;
using System.Threading.Tasks;
using NUnit.Framework;
using Rebus.Activation;
using Rebus.Config;
using Rebus.Routing.TypeBased;
using Rebus.Tests.Contracts;
using Rebus.Tests.Contracts.Utilities;
using Rebus.Transport;
// ReSharper disable ArgumentsStyleLiteral

#pragma warning disable 1998

namespace Rebus.SqlServer.Tests.Transport
{
    [TestFixture]
    public class TestNewTransport : FixtureBase
    {
        protected override void SetUp() => SqlTestHelper.DropAllTables();

        [Test]
        public async Task TheNewTransportWorks()
        {
            var counter = Using(new SharedCounter(10));
            var activator = Using(new BuiltinHandlerActivator());

            activator.Handle<string>(async _ => counter.Decrement());

            Configure.With(activator)
                .Transport(t => t.UseSqlServerNew(SqlTestHelper.ConnectionString, "test-queue"))
                .Start();

            var client = Using(
                Configure.With(new BuiltinHandlerActivator())
                    .Transport(t => t.UseSqlServerAsOneWayClientNew(SqlTestHelper.ConnectionString))
                    .Routing(r => r.TypeBased().Map<string>("test-queue"))
                    .Start()
            );

            await Task.WhenAll(Enumerable.Range(0, 10).Select(i => client.Send($"Message number {i}")));

            counter.WaitForResetEvent(timeoutSeconds: 2);
        }

        [Test]
        public async Task TheNewTransportWorks_BatchSend()
        {
            var counter = Using(new SharedCounter(10));
            var activator = Using(new BuiltinHandlerActivator());

            activator.Handle<string>(async _ => counter.Decrement());

            Configure.With(activator)
                .Transport(t => t.UseSqlServerNew(SqlTestHelper.ConnectionString, "test-queue"))
                .Start();

            var client = Using(
                Configure.With(new BuiltinHandlerActivator())
                    .Transport(t => t.UseSqlServerAsOneWayClientNew(SqlTestHelper.ConnectionString))
                    .Routing(r => r.TypeBased().Map<string>("test-queue"))
                    .Start()
            );

            var messages = Enumerable.Range(0, 10).Select(i => $"Message number {i}");

            using (var scope = new RebusTransactionScope())
            {
                foreach (var message in messages)
                {
                    await client.Send(message);
                }

                await scope.CompleteAsync();
            }

            counter.WaitForResetEvent(timeoutSeconds: 2);
        }
    }
}