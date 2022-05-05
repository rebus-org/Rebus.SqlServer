using System;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using Rebus.Activation;
using Rebus.Config;
using Rebus.Config.Outbox;
using Rebus.Tests.Contracts;
using Rebus.Transport;
using Rebus.Transport.InMem;
// ReSharper disable AccessToDisposedClosure
#pragma warning disable CS1998

namespace Rebus.SqlServer.Tests.Outbox;

[TestFixture]
[Description("When everything clicks, except the final ACK of having received & processed a message, we will receive the same message again. We simulate that here.")]
public class TestOutboxIdempotency : FixtureBase
{
    [Test]
    public async Task DoesNotProcessSameMessageTwice()
    {
        SqlTestHelper.DropTable("Outbox");

        using var countdown = new CountdownEvent(initialCount: 2);
        using var adapter = new BuiltinHandlerActivator();

        adapter.Handle<MyMessage>(async _ => countdown.Signal());

        var bus = Configure.With(adapter)
            .Transport(t => t.UseInMemoryTransport(new InMemNetwork(), "idempotency"))
            .Options(o => o.Decorate<ITransport>(c => new FlakyAckerTransportDecorator(c.Get<ITransport>(), ackFailures: 1)))
            .Outbox(o => o.StoreInSqlServer(SqlTestHelper.ConnectionString, "Outbox"))
            .Start();

        await bus.SendLocal(new MyMessage());

        if (countdown.Wait(TimeSpan.FromSeconds(2)))
        {
            throw new AssertionException(
                "Expected the countdown event to get a timeout, but it was decremented twice, which means that the message was handled twice!!!");
        }
    }

    record MyMessage;
}