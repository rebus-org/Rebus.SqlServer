using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Rebus.Bus;
using Rebus.Logging;
using Rebus.Messages;
using Rebus.Pipeline;
using Rebus.Threading;
using Rebus.Transport;
#pragma warning disable CS1998

namespace Rebus.SqlServer.Outbox;

class OutboxTransportDecorator : ITransport, IInitializable, IDisposable
{
    public const string BypassOutboxKey = "rebus-bypass-outbox";

    readonly OutboxForwarder _forwarder;
    readonly IOutboxStorage _outboxStorage;
    readonly ITransport _transport;

    public OutboxTransportDecorator(IRebusLoggerFactory rebusLoggerFactory, ITransport transport, IOutboxStorage outboxStorage, IAsyncTaskFactory asyncTaskFactory)
    {
        _forwarder = new OutboxForwarder(asyncTaskFactory, rebusLoggerFactory, outboxStorage, transport);
        _transport = transport;
        _outboxStorage = outboxStorage;
    }

    public void Initialize()
    {
        _forwarder.Start();
    }

    public void CreateQueue(string address) => _transport.CreateQueue(address);

    public async Task Send(string destinationAddress, TransportMessage message, ITransactionContext context)
    {
        if (context.Items.ContainsKey(BypassOutboxKey))
        {
            await _transport.Send(destinationAddress, message, context);
            return;
        }

        ConcurrentQueue<AbstractRebusTransport.OutgoingMessage> SendOutgoingMessages()
        {
            var queue = new ConcurrentQueue<AbstractRebusTransport.OutgoingMessage>();
            var correlationId = Guid.NewGuid().ToString("N").Substring(0, 16);

            // if we're currently handling a message, we get information about it here
            if (context.Items.TryGetValue("stepContext", out var result) && result is IncomingStepContext stepContext)
            {
                var messageId = stepContext.Load<TransportMessage>().GetMessageId();
                var sourceQueue = _transport.Address;

                async Task CommitAction(ITransactionContext _)
                {
                    if (!queue.Any()) return; //< don't do anything if no outgoing messages were sent

                    await _outboxStorage.Save(outgoingMessages: queue, messageId: messageId, sourceQueue: sourceQueue, correlationId);
                }

                context.OnCommitted(CommitAction);
            }
            else
            {
                async Task CommitAction(ITransactionContext _)
                {
                    if (!queue.Any()) return; //< don't do anything if no outgoing messages were sent

                    await _outboxStorage.Save(outgoingMessages: queue, correlationId: correlationId);
                }

                context.OnCommitted(CommitAction);
            }

            // here, we intentionally kick off an async task to try to eager-send the messages that were just sent
            context.OnCompleted(async _ => _forwarder.TryEagerSend(queue, correlationId));

            return queue;
        }

        var outgoingMessages = context.GetOrAdd("rebus-outbox-messages", SendOutgoingMessages);

        outgoingMessages.Enqueue(new AbstractRebusTransport.OutgoingMessage(message, destinationAddress));
    }

    public Task<TransportMessage> Receive(ITransactionContext context, CancellationToken cancellationToken) => _transport.Receive(context, cancellationToken);

    public string Address => _transport.Address;

    public void Dispose()
    {
        _forwarder.Dispose();
    }
}
