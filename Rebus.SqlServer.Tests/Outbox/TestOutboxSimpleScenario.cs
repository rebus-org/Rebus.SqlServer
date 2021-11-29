using System;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using Rebus.Activation;
using Rebus.Config;
using Rebus.Config.Outbox;
using Rebus.Tests.Contracts;
using Rebus.Tests.Contracts.Extensions;
using Rebus.Transport;
using Rebus.Transport.InMem;
#pragma warning disable 1998

namespace Rebus.SqlServer.Tests.Outbox;

[TestFixture]
public class TestOutboxSimpleScenario : FixtureBase
{
    [Test]
    public async Task CanDoIt_NotUsingOutbox()
    {
        using var activator = new BuiltinHandlerActivator();

        using var gotTheString = new ManualResetEvent(initialState: false);

        activator.Handle<string>(async msg => gotTheString.Set());

        var bus = Configure.With(activator)
            .Transport(t => t.UseInMemoryTransport(new InMemNetwork(), "doesn't matter"))
            .Outbox(o => o.UseSqlServer(SqlTestHelper.ConnectionString, "Outbox"))
            .Start();

        using (var scope = new RebusTransactionScope())
        {
            await bus.SendLocal("HEJ MED DIG 🙂");

            await scope.CompleteAsync();
        }

        gotTheString.WaitOrDie(TimeSpan.FromSeconds(5));
    }

    [Test]
    public async Task CanDoIt_UsingOutbox()
    {
        using var gotTheString = new ManualResetEvent(initialState: false);

        using var activator = new BuiltinHandlerActivator();

        activator.Handle<string>(async msg => gotTheString.Set());

        var bus = Configure.With(activator)
            .Transport(t =>
            {
                t.UseInMemoryTransport(new InMemNetwork(), "doesn't matter");
                t.ThrowSometimesWhenSendingMessages(successRate: 0.5);
            })
            .Outbox(o => o.UseSqlServer(SqlTestHelper.ConnectionString, "Outbox"))
            .Start();

        using (var scope = new RebusTransactionScope())
        {
            await bus.SendLocal("HEJ MED DIG 🙂");

            await scope.CompleteAsync();
        }

        gotTheString.WaitOrDie(TimeSpan.FromSeconds(15));
    }
}