using System;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using Rebus.Activation;
using Rebus.Config;
using Rebus.Tests.Contracts;
using Rebus.Tests.Contracts.Extensions;

#pragma warning disable 1998

namespace Rebus.SqlServer.Tests.Integration
{
    [TestFixture, Category(Categories.SqlServer)]
    public class TestSqlAllTheWay : FixtureBase
    {
        static readonly string ConnectionString = SqlTestHelper.ConnectionString;

        BuiltinHandlerActivator _activator;
        IBusStarter _starter;

        protected override void SetUp()
        {
            DropTables();

            _activator = new BuiltinHandlerActivator();

            Using(_activator);

            _starter = Configure.With(_activator)
                .Transport(x => x.UseSqlServer(new SqlServerTransportOptions(ConnectionString), "test.input"))
                .Sagas(x => x.StoreInSqlServer(ConnectionString, "Sagas", "SagaIndex"))
                .Options(x =>
                {
                    x.SetNumberOfWorkers(1);
                    x.SetMaxParallelism(1);
                })
                .Create();
        }

        protected override void TearDown()
        {
            DropTables();
        }

        static void DropTables()
        {
            SqlTestHelper.DropTable("RebusMessages");
            SqlTestHelper.DropTable("SagaIndex");
            SqlTestHelper.DropTable("Sagas");
        }

        [Test]
        public async Task SendAndReceiveOneSingleMessage()
        {
            var gotTheMessage = new ManualResetEvent(false);
            var receivedMessageCount = 0;

            _activator.Handle<string>(async message =>
            {
                Interlocked.Increment(ref receivedMessageCount);
                Console.WriteLine("w00000t! Got message: {0}", message);
                gotTheMessage.Set();
            });

            var bus = _starter.Start();
            await bus.SendLocal("hej med dig min ven!");

            gotTheMessage.WaitOrDie(TimeSpan.FromSeconds(10));

            await Task.Delay(500);

            Assert.That(receivedMessageCount, Is.EqualTo(1));
        }
    }
}
