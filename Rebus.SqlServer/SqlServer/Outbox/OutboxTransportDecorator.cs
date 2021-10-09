using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using Rebus.Messages;
using Rebus.Transport;

namespace Rebus.SqlServer.Outbox
{
    class OutboxTransportDecorator : ITransport
    {
        public const string OutboxEnabledKey = "rebus-outbox-enabled";

        private readonly ITransport _transport;
        private readonly IOutboxStorage _outboxStorage;

        public OutboxTransportDecorator(ITransport transport, IOutboxStorage outboxStorage)
        {
            _transport = transport;
            _outboxStorage = outboxStorage;
        }

        public void CreateQueue(string address) => _transport.CreateQueue(address);

        public async Task Send(string destinationAddress, TransportMessage message, ITransactionContext context)
        {
            if (!context.Items.ContainsKey(OutboxEnabledKey))
            {
                await _transport.Send(destinationAddress, message, context);
                return;
            }

            var outgoingMessages = context.GetOrAdd("outbox-messages", () =>
            {
                var queue = new ConcurrentQueue<AbstractRebusTransport.OutgoingMessage>();

                context.OnCommitted(async _ => await _outboxStorage.Save(queue));

                context.OnCompleted(async => Task.Run(async () =>
                {

                }));

                return queue;
            });

            outgoingMessages.Enqueue(new AbstractRebusTransport.OutgoingMessage(message, destinationAddress));

            //await _transport.Send(destinationAddress, message, context);
        }

        public async Task<TransportMessage> Receive(ITransactionContext context, CancellationToken cancellationToken)
        {
            return await _transport.Receive(context, cancellationToken);
        }

        public string Address => _transport.Address;
    }
}
