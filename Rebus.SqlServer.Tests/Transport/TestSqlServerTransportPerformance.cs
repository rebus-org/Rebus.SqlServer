using System;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using NUnit.Framework;
using Rebus.SqlServer.Tests.Extensions;
using Rebus.SqlServer.Tests.Transport.Contract.Factories;
using Rebus.Tests.Contracts;
using Rebus.Tests.Contracts.Transports;
using Rebus.Tests.Contracts.Utilities;
// ReSharper disable ArgumentsStyleNamedExpression

#pragma warning disable 1998

namespace Rebus.SqlServer.Tests.Transport
{
    [TestFixture]
    public class TestSqlServerTransportPerformance : FixtureBase
    {
        [TestCase(100, 10, typeof(SqlServerLeaseBusFactory))]
        [TestCase(100, 10, typeof(NewSqlServerBusFactory))]
        [TestCase(100, 10, typeof(SqlServerBusFactory))]
        [TestCase(1000, 30, typeof(SqlServerLeaseBusFactory))]
        [TestCase(1000, 30, typeof(NewSqlServerBusFactory))]
        [TestCase(1000, 30, typeof(SqlServerBusFactory))]
        [TestCase(10000, 60, typeof(SqlServerLeaseBusFactory))]
        [TestCase(10000, 60, typeof(NewSqlServerBusFactory))]
        [TestCase(10000, 60, typeof(SqlServerBusFactory))]
        public async Task Run(int messageCount, int receiveTimeoutSeconds, Type factoryType)
        {
            var factory = (IBusFactory)Activator.CreateInstance(factoryType);

            try
            {
                var counter = Using(new SharedCounter(messageCount));

                var sender = factory.GetBus<string>("sender", async _ => { });
                var receiver = factory.GetBus<string>("receiver", async message => counter.Decrement());

                receiver.Advanced.Workers.SetNumberOfWorkers(0);

                var stopwatch = Stopwatch.StartNew();

                await Task.WhenAll(
                    Enumerable.Range(0, messageCount)
                        .Select(n => sender.Advanced.Routing.Send("receiver", $"THIS IS MESSAGE NUMBER {n}"))
                );

                var elapsedSending = stopwatch.GetLapTime();

                receiver.Advanced.Workers.SetNumberOfWorkers(10);
                
                counter.WaitForResetEvent(timeoutSeconds: receiveTimeoutSeconds);
                
                var elapsedReceiving = stopwatch.GetLapTime();

                Console.WriteLine($@"

Sending {messageCount} messages took {elapsedSending.TotalSeconds:0.0} s - that's {messageCount / elapsedSending.TotalSeconds:0.0} msg/s

Receiving {messageCount} messages took {elapsedReceiving.TotalSeconds:0.0} s - that's {messageCount / elapsedReceiving.TotalSeconds:0.0} msg/s

");
            }
            finally
            {
                factory.Cleanup();
            }
        }
    }
}