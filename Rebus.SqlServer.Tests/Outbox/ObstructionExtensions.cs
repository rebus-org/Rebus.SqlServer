using System;
using System.Threading;
using System.Threading.Tasks;
using Rebus.Config;
using Rebus.Messages;
using Rebus.Transport;

namespace Rebus.SqlServer.Tests.Outbox
{
    static class ObstructionExtensions
    {
        public static void ThrowSometimesWhenSendingMessages(this StandardConfigurer<ITransport> configurer, double successRate = 0.5)
        {
            configurer.Decorate(t => new ThrowSometimesWhenSendingTransportDecorator(t.Get<ITransport>(), successRate));
        }

        class ThrowSometimesWhenSendingTransportDecorator : ITransport
        {
            readonly ThreadLocal<Random> _random = new(() => new Random());
            readonly ITransport _transport;
            readonly double _successRate;

            public ThrowSometimesWhenSendingTransportDecorator(ITransport transport, double successRate)
            {
                _transport = transport;
                _successRate = successRate;
            }

            public void CreateQueue(string address) => _transport.CreateQueue(address);

            public Task Send(string destinationAddress, TransportMessage message, ITransactionContext context) =>
                _random.Value.NextDouble() < _successRate
                    ? _transport.Send(destinationAddress, message, context)
                    : throw new ApplicationException("OBSTRUCTION");

            public Task<TransportMessage> Receive(ITransactionContext context, CancellationToken cancellationToken) => _transport.Receive(context, cancellationToken);

            public string Address => _transport.Address;
        }
    }
}
