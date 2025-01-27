using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading.Tasks;
using NUnit.Framework;
using Rebus.Activation;
using Rebus.Config;
using Rebus.Logging;
using Rebus.SqlServer.Tests.Extensions;
using Rebus.Tests.Contracts;

#pragma warning disable CS1998

namespace Rebus.SqlServer.Tests.Bugs;

[TestFixture]
public class TestLeaseBasedTransportAndConcurrency : FixtureBase
{
    [TestCase(2, 2, 10)]
    [TestCase(5, 10, 100)]
    [TestCase(5, 10, 1000, 10)]
    [TestCase(5, 10, 10000, 30, Explicit = true)]
    [Repeat(10)]
    [Description(@"Tried (without success) to reproduce a situation where the same message seemed to be received twice, allegedly because of SQL Server failing to properly handling the exclusive lock&update required to grab a lease on the message. 

It did not look like this was the case though, as everything seemed to work just like it should.")]
    public async Task TryToReproduceTheIssue(int numberOfBusInstances, int parallelismPerInstance, int messageCount, int timeoutSeconds = 5)
    {
        var inputQueueName = Guid.NewGuid().ToString("N");

        Using(new DisposableCallback(() => SqlTestHelper.DropTable(inputQueueName)));

        var receiveCountsByMessageId = new ConcurrentDictionary<string, int>();

        var instances = Enumerable
            .Range(0, numberOfBusInstances)
            .Select(_ =>
            {
                var activator = Using(new BuiltinHandlerActivator());

                activator.Handle<MessageWithId>(async msg => receiveCountsByMessageId.AddOrUpdate(msg.Id, 1, (_, count) => count + 1));

                Configure.With(activator)
                    .Logging(l => l.Console(minLevel: LogLevel.Warn))
                    .Transport(t => t.UseSqlServerInLeaseMode(new SqlServerLeaseTransportOptions(SqlTestHelper.ConnectionString), inputQueueName))
                    .Options(o =>
                    {
                        o.SetNumberOfWorkers(0); //< start out with passive instances
                        o.SetMaxParallelism(parallelismPerInstance);
                    })
                    .Start();

                return activator.Bus;
            })
            .ToList();

        // optimistic attempt at disposing in parallel, because it's quicker
        Using(new DisposableCallback(() => Parallel.ForEach(instances, i => i.Dispose()))); ;

        var messages = Enumerable
            .Range(0, messageCount)
            .Select(n => new MessageWithId($"MESSAGE NUMBER {n}"));

        var sender = instances.First();

        await Parallel.ForEachAsync(messages, async (msg, _) => await sender.SendLocal(msg));

        // start all the workers
        Parallel.ForEach(instances, bus => bus.Advanced.Workers.SetNumberOfWorkers(parallelismPerInstance));

        await receiveCountsByMessageId.WaitUntil(c => c.Count == messageCount, timeoutSeconds: timeoutSeconds);

        Assert.That(receiveCountsByMessageId.All(c => c.Value == 1), Is.True, $@"One or more messages were received more than once:

{string.Join(Environment.NewLine, receiveCountsByMessageId.Where(c => c.Value != 1).Select(kvp => $"    {kvp.Key}: {kvp.Value}"))}");
    }

    record MessageWithId(string Id);
}