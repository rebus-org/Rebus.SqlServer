using System;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using Rebus.Activation;
using Rebus.Config;
using Rebus.Config.Outbox;
using Rebus.Messages;
using Rebus.Tests.Contracts;
using Rebus.Tests.Contracts.Extensions;
using Rebus.Transport;
using Rebus.Transport.InMem;
#pragma warning disable 1998

namespace Rebus.SqlServer.Tests.Outbox
{
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
                .Transport(t => t.UseInMemoryTransport(new InMemNetwork(), "doesn't matter"))
                //.Options(o => o.ThrowWhenSendingMessages())
                .Outbox(o => o.UseSqlServer(SqlTestHelper.ConnectionString, "Outbox"))
                .Start();

            using (var scope = new RebusTransactionScope())
            {
                scope.UseOutbox();

                await bus.SendLocal("HEJ MED DIG 🙂");

                await scope.CompleteAsync();
            }

            gotTheString.WaitOrDie(TimeSpan.FromSeconds(15));
        }
    }

    static class ObstructionExtensions
    {
        public static void ThrowWhenSendingMessages(this OptionsConfigurer configurer)
        {
            configurer.Decorate<ITransport>(t => new ThrowWhenSendingTransportDecorator(t.Get<ITransport>()));
        }

        class ThrowWhenSendingTransportDecorator : ITransport
        {
            private readonly ITransport _transport;

            public ThrowWhenSendingTransportDecorator(ITransport transport) => _transport = transport;

            public void CreateQueue(string address) => _transport.CreateQueue(address);

            public async Task Send(string destinationAddress, TransportMessage message, ITransactionContext context) => throw new ApplicationException("CANNOT SEND");

            public Task<TransportMessage> Receive(ITransactionContext context, CancellationToken cancellationToken) => _transport.Receive(context, cancellationToken);

            public string Address => _transport.Address;
        }
    }
}
