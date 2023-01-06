using System;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using NUnit.Framework;
using Rebus.Activation;
using Rebus.Config;
using Rebus.Logging;
using Rebus.Tests.Contracts;
using Rebus.Transport;

namespace Rebus.SqlServer.Tests.Transport;

[TestFixture]
public class TestSqlTransportSendPerformance : FixtureBase
{
    [TestCase(100, 10)]
    [TestCase(1000, 10)]
    [TestCase(10000, 10)]
    public async Task SendLotsOfMessages(int count, int batchSize)
    {
        var stopwatch = Stopwatch.StartNew();

        var queueName = Guid.NewGuid().ToString("N");

        Using(new DisposableCallback(() => SqlTestHelper.DropTable(queueName)));

        using var inactiveBus = Configure.With(new BuiltinHandlerActivator())
            .Logging(l => l.Console(minLevel: LogLevel.Warn))
            .Transport(t => t.UseSqlServer(new(SqlTestHelper.ConnectionString), queueName))
            .Options(o => o.SetNumberOfWorkers(0))
            .Start();

        var messages = Enumerable.Range(0, count)
            .Select(n => new SomeKindOfMessage($"THIS IS MESSAGE NUMBER {n}"));

        await Parallel.ForEachAsync(
            source: messages.Batch(batchSize),
            body: async (batch, _) =>
            {
                using var scope = new RebusTransactionScope();

                foreach (var message in batch)
                {
                    await inactiveBus.SendLocal(message);
                }

                await scope.CompleteAsync();
            }
        );

        var elapsed = stopwatch.Elapsed;

        Console.WriteLine($"Sending {count} messages took {elapsed.TotalSeconds:0.0} s - that's {count / elapsed.TotalSeconds:0.0} msg/s");
    }

    record SomeKindOfMessage(string Text);
}