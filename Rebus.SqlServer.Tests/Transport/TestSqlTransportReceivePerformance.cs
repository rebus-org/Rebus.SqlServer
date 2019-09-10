using System;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using NUnit.Framework;
using Rebus.Activation;
using Rebus.Config;
using Rebus.Logging;
using Rebus.Tests.Contracts;
using Rebus.Tests.Contracts.Utilities;
// ReSharper disable ArgumentsStyleOther

#pragma warning disable 1998

namespace Rebus.SqlServer.Tests.Transport
{
    [TestFixture, Category(Categories.SqlServer)]
    public class TestSqlTransportReceivePerformance : FixtureBase
    {
        const string QueueName = "perftest";

        static readonly string TableName = TestConfig.GetName("perftest");

        protected override void SetUp()
        {
            SqlTestHelper.DropTable(TableName);
        }

        /*
         *
Can be used as a simple indicator of the receive performance of the SQL transport.

Before PR 55:
    *** Using LEASE-BASED SQL transport ***
    1000 messages received in 6,5 s - that's 152,7 msg/s

    *** Using LEASE-BASED SQL transport ***
    10000 messages received in 56,3 s - that's 177,7 msg/s

After PR 55:
    *** Using LEASE-BASED SQL transport ***
    1000 messages received in 2,0 s - that's 499,4 msg/s

    *** Using LEASE-BASED SQL transport ***
    10000 messages received in 41,2 s - that's 242,7 msg/s
         *
         */
        [TestCase(1000, true)]
        [TestCase(1000, false)]
        [TestCase(10000, true)]
        [TestCase(10000, false)]
        public async Task CheckReceivePerformance(int messageCount, bool useLeaseBasedTransport)
        {
            var adapter = Using(new BuiltinHandlerActivator());

            Configure.With(adapter)
                .Logging(l => l.ColoredConsole(LogLevel.Warn))
                .Transport(t =>
                {
                    if (useLeaseBasedTransport)
                    {
                        Console.WriteLine("*** Using LEASE-BASED SQL transport ***");
                        t.UseSqlServerInLeaseMode(new SqlServerLeaseTransportOptions(SqlTestHelper.ConnectionString), QueueName);
                    }
                    else
                    {
                        Console.WriteLine("*** Using NORMAL SQL transport ***");
                        t.UseSqlServer(new SqlServerTransportOptions(SqlTestHelper.ConnectionString), QueueName);
                    }
                })
                .Options(o =>
                {
                    o.SetNumberOfWorkers(0);
                    o.SetMaxParallelism(20);
                })
                .Start();

            Console.WriteLine($"Sending {messageCount} messages...");

            await Task.WhenAll(Enumerable.Range(0, messageCount)
                .Select(i => adapter.Bus.SendLocal($"THIS IS MESSAGE {i}")));

            var counter = Using(new SharedCounter(messageCount));

            adapter.Handle<string>(async message => counter.Decrement());

            Console.WriteLine("Waiting for messages to be received...");

            var stopwtach = Stopwatch.StartNew();

            adapter.Bus.Advanced.Workers.SetNumberOfWorkers(3);

            counter.WaitForResetEvent(timeoutSeconds: messageCount / 100 + 5);

            var elapsedSeconds = stopwtach.Elapsed.TotalSeconds;

            Console.WriteLine($"{messageCount} messages received in {elapsedSeconds:0.0} s - that's {messageCount / elapsedSeconds:0.0} msg/s");
        }
    }
}
