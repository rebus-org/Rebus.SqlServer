using System;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using Rebus.Activation;
using Rebus.Config;
using Rebus.Config.Outbox;
using Rebus.Exceptions;
using Rebus.Retry.Simple;
using Rebus.Routing.TypeBased;
using Rebus.Tests.Contracts;
using Rebus.Tests.Contracts.Extensions;
using Rebus.Transport.InMem;
// ReSharper disable AccessToDisposedClosure
#pragma warning disable CS1998 // Async method lacks 'await' operators and will run synchronously

namespace Rebus.SqlServer.Tests.Bugs;

[TestFixture]
[Description(@"When outbox and 2nd level retries are combined, it's important that outgoing messages from 1st level handler do not get sent 
when 1st level handler throws an exception. It used to be the case that when 1st level handler threw, but 2nd level handler SUCCEEDED, then
outbox messages sent by 1st level handler would end up getting sent. This is no longer the case, as verified by this test.")]
public class TestBugWhereOutboxMessagesAreSentWhenSecondLevelRetryHandlerCompletes : FixtureBase
{
    string _outboxTable;
    InMemNetwork _network;

    protected override void SetUp()
    {
        _network = new InMemNetwork();

        _outboxTable = "outbox-buggerino";

        SqlTestHelper.DropTable(_outboxTable);

        Using(new DisposableCallback(() => SqlTestHelper.DropTable(_outboxTable)));
    }

    [Test]
    public async Task Reproduce()
    {
        _network.CreateQueue("destination");
        _network.CreateQueue("final");

        using var done = new ManualResetEvent(initialState: false);
        using var activator = new BuiltinHandlerActivator();

        activator.Handle<Initiator>(async (bus, _) =>
        {
            await bus.Send(new MessageForDestination());
            throw new FailFastException("💀");
        });

        activator.Handle<IFailed<Initiator>>(async (bus, _) =>
        {
            await bus.Send(new FinalMessage());
            done.Set();
        });

        Configure.With(activator)
            .Transport(t => t.UseInMemoryTransport(_network, "whatever"))
            .Outbox(o => o.StoreInSqlServer(SqlTestHelper.ConnectionString, _outboxTable))
            .Routing(r => r.TypeBased()
                .Map<MessageForDestination>("destination")
                .Map<FinalMessage>("final"))
            .Options(o => o.RetryStrategy(secondLevelRetriesEnabled: true))
            .Start();

        await activator.Bus.SendLocal(new Initiator());

        done.WaitOrDie(TimeSpan.FromSeconds(50));

        await Task.Delay(TimeSpan.FromSeconds(2));

        var destinationQueueMessageCount = _network.Count("destination");
        var finalQueueMessageCount = _network.Count("final");

        Console.WriteLine($@"Queue counts:

    'destination': {destinationQueueMessageCount}
          'final': {finalQueueMessageCount}
");

        Assert.That(destinationQueueMessageCount, Is.Zero);
        Assert.That(finalQueueMessageCount, Is.EqualTo(1));
    }

    record Initiator;

    record MessageForDestination;
    
    record FinalMessage;
}