using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using Rebus.Config.Outbox;
using Rebus.Messages;
using Rebus.Pipeline;
using Rebus.Retry.Simple;
using Rebus.Transport;
// ReSharper disable ArgumentsStyleLiteral

namespace Rebus.SqlServer.Outbox;

class OutboxClientTransportDecorator : ITransport
{
    const string OutgoingMessagesKey = "outbox-outgoing-messages";
    const string SecondLevelOutgoingMessagesKey = "outbox-outgoing-messages/2nd-level";

    readonly ITransport _transport;
    readonly IOutboxStorage _outboxStorage;

    public OutboxClientTransportDecorator(ITransport transport, IOutboxStorage outboxStorage)
    {
        _transport = transport ?? throw new ArgumentNullException(nameof(transport));
        _outboxStorage = outboxStorage ?? throw new ArgumentNullException(nameof(outboxStorage));
    }

    public void CreateQueue(string address) => _transport.CreateQueue(address);

    public Task Send(string destinationAddress, TransportMessage message, ITransactionContext context)
    {
        var connection = context.GetOrNull<OutboxConnection>(OutboxExtensions.CurrentOutboxConnectionKey);

        if (connection == null)
        {
            return _transport.Send(destinationAddress, message, context);
        }

        var key = GetOutgoingMessagesKey(context);

        ConcurrentQueue<OutgoingTransportMessage> NewItemFactoryingMessages()
        {
            var queue = new ConcurrentQueue<OutgoingTransportMessage>();
            var dbConnectionWrapper = new DbConnectionWrapper(connection.Connection, connection.Transaction, managedExternally: true);
            context.OnCommit(async _ => await _outboxStorage.Save(queue, dbConnectionWrapper));
            return queue;
        }

        var outgoingMessages = context.GetOrAdd(key, () =>
        {
            var queue = new ConcurrentQueue<OutgoingTransportMessage>();
            var dbConnectionWrapper = new DbConnectionWrapper(connection.Connection, connection.Transaction, managedExternally: true);
            
            context.OnCommit(async _ =>
            {
                // if this particular commit callback is being called for outbox messages sent in 1st level handler, but we have advanced to
                // dispatching as 2nd level, then we must ignore the messages!
                var theseOutboxMessagesAreFirstLevelOutboxMessages = key == OutgoingMessagesKey;
                var messageIsBeingHandledBySecondLevelRetryHandler = SecondLevelRetryInProgress(context);

                if (theseOutboxMessagesAreFirstLevelOutboxMessages && messageIsBeingHandledBySecondLevelRetryHandler) return;

                await _outboxStorage.Save(queue, dbConnectionWrapper);
            });
            return queue;
        });

        outgoingMessages.Enqueue(new OutgoingTransportMessage(message, destinationAddress));

        return Task.CompletedTask;
    }

    static string GetOutgoingMessagesKey(ITransactionContext context) => SecondLevelRetryInProgress(context) ? SecondLevelOutgoingMessagesKey : OutgoingMessagesKey;

    static bool SecondLevelRetryInProgress(ITransactionContext context)
    {
        // if there's an incoming step context
        if (context.Items.TryGetValue(StepContext.StepContextKey, out var stepContext) && stepContext is IncomingStepContext incomingStepContext)
        {
            // ...and we're currently dispatching as a 2nd level retry, use another key for the outgoing messages
            return incomingStepContext.Load<bool>(DefaultRetryStep.DispatchAsFailedMessageKey);
        }

        return false;
    }

    public Task<TransportMessage> Receive(ITransactionContext context, CancellationToken cancellationToken) => _transport.Receive(context, cancellationToken);

    public string Address => _transport.Address;
}