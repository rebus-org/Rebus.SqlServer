using System;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using Rebus.Activation;
using Rebus.Bus;
using Rebus.Config;
using Rebus.Config.Outbox;
using Rebus.Pipeline;
using Rebus.Routing;
using Rebus.Routing.TypeBased;
using Rebus.Tests.Contracts;
using Rebus.Tests.Contracts.Extensions;
using Rebus.Transport;
using Rebus.Transport.InMem;

// ReSharper disable ArgumentsStyleLiteral
// ReSharper disable AccessToDisposedClosure
#pragma warning disable CS1998

namespace Rebus.SqlServer.Tests.Outbox;

[TestFixture]
public class TestOutbox_InsideRebusHandler : FixtureBase
{
    static string ConnectionString => SqlTestHelper.ConnectionString;

    InMemNetwork _network;

    protected override void SetUp()
    {
        base.SetUp();

        SqlTestHelper.DropTable("RebusOutbox");

        _network = new InMemNetwork();
    }

    record SomeMessage;

    record AnotherMessage;

    [Test]
    public async Task CanHandleMessageAndSendOutgoingMessagesEvenWhenTransportIsFlaky()
    {
        using var gotSomeMessage = new ManualResetEvent(initialState: false);
        using var gotAnotherMessage = new ManualResetEvent(initialState: false);

        var flakySenderTransportDecoratorSettings = new FlakySenderTransportDecoratorSettings();

        async Task HandlerFunction(IBus bus, IMessageContext context, SomeMessage message)
        {
            await bus.Advanced.Routing.Send("secondConsumer", new AnotherMessage());

            gotSomeMessage.Set();
        }

        using var firstConsumer = CreateConsumer("firstConsumer", activator => activator.Handle<SomeMessage>(HandlerFunction), flakySenderTransportDecoratorSettings);
        using var secondConsumer = CreateConsumer("secondConsumer", activator => activator.Handle<AnotherMessage>(async _ => gotAnotherMessage.Set()));

        using var client = CreateOneWayClient(router => router.TypeBased().Map<SomeMessage>("firstConsumer"));

        // make it so that the first consumer cannot send
        flakySenderTransportDecoratorSettings.SuccessRate = 0;

        await client.Send(new SomeMessage());

        // wait for SomeMessage to be handled
        gotSomeMessage.WaitOrDie(timeout: TimeSpan.FromSeconds(3));

        // now make it possible for first consumer to send again
        flakySenderTransportDecoratorSettings.SuccessRate = 1;

        // wait for AnotherMessage to arrive
        gotAnotherMessage.WaitOrDie(timeout: TimeSpan.FromSeconds(15));
    }

    IBus CreateConsumer(string queueName, Action<BuiltinHandlerActivator> handlers = null, FlakySenderTransportDecoratorSettings flakySenderTransportDecoratorSettings = null)
    {
        var activator = new BuiltinHandlerActivator();

        handlers?.Invoke(activator);

        Configure.With(activator)
            .Transport(t =>
            {
                t.UseInMemoryTransport(_network, queueName);

                if (flakySenderTransportDecoratorSettings != null)
                {
                    t.Decorate(c => new FlakySenderTransportDecorator(c.Get<ITransport>(),
                        flakySenderTransportDecoratorSettings));
                }
            })
            .Outbox(o => o.StoreInSqlServer(ConnectionString, "RebusOutbox"))
            .Start();

        return activator.Bus;
    }

    IBus CreateOneWayClient(Action<StandardConfigurer<IRouter>> routing = null)
    {
        return Configure.With(new BuiltinHandlerActivator())
            .Transport(t => t.UseInMemoryTransportAsOneWayClient(_network))
            .Routing(r => routing?.Invoke(r))
            .Start();
    }

}