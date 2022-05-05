using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using Rebus.Config.Outbox;
using Rebus.Messages;
using Rebus.Transport;
// ReSharper disable ArgumentsStyleLiteral

namespace Rebus.SqlServer.Outbox;

class OutboxClientTransportDecorator : ITransport
{
    const string OutgoingMessagesKey = "outbox-outgoing-messages";
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

        var outgoingMessages = context.GetOrAdd(OutgoingMessagesKey, () =>
        {
            var queue = new ConcurrentQueue<AbstractRebusTransport.OutgoingMessage>();
            var dbConnectionWrapper = new DbConnectionWrapper(connection.Connection, connection.Transaction, managedExternally: true);
            context.OnCommitted(async _ => await _outboxStorage.Save(dbConnectionWrapper, queue));
            return queue;
        });

        outgoingMessages.Enqueue(new AbstractRebusTransport.OutgoingMessage(message, destinationAddress));

        return Task.CompletedTask;
    }

    public Task<TransportMessage> Receive(ITransactionContext context, CancellationToken cancellationToken) => _transport.Receive(context, cancellationToken);

    public string Address => _transport.Address;
}