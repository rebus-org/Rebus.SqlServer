using System;
using System.Collections.Concurrent;
using System.Threading.Tasks;
using Rebus.Bus;
using Rebus.Config.Outbox;
using Rebus.Messages;
using Rebus.Pipeline;
using Rebus.Transport;
#pragma warning disable CS1998

namespace Rebus.SqlServer.Outbox;

class OutboxIncomingStep : IIncomingStep
{
    readonly IOutboxConnectionProvider _outboxConnectionProvider;
    readonly IOutboxStorage _outboxStorage;
    readonly ITransport _transport;

    public OutboxIncomingStep(IOutboxConnectionProvider outboxConnectionProvider, IOutboxStorage outboxStorage, ITransport transport)
    {
        _outboxConnectionProvider = outboxConnectionProvider ?? throw new ArgumentNullException(nameof(outboxConnectionProvider));
        _outboxStorage = outboxStorage ?? throw new ArgumentNullException(nameof(outboxStorage));
        _transport = transport ?? throw new ArgumentNullException(nameof(transport));
    }

    public async Task Process(IncomingStepContext context, Func<Task> next)
    {
        var outboxConnection = _outboxConnectionProvider.GetDbConnection();
        var transactionContext = context.Load<ITransactionContext>();
        var message = context.Load<TransportMessage>();
        var messageId = message.GetMessageId();

        var connection = new DbConnectionWrapper(outboxConnection.Connection, outboxConnection.Transaction, managedExternally: true);

        // idempotency check
        if (await _outboxStorage.HasProcessedMessage(connection, _transport.Address, messageId)) return;

        await _outboxStorage.MarkMessageAsProcessed(connection, _transport.Address, messageId);
        
        transactionContext.Items[OutboxExtensions.CurrentOutboxConnectionKey] = outboxConnection;

        transactionContext.OnCommitted(async _ => outboxConnection.Transaction.Commit());

        transactionContext.OnDisposed(_ =>
        {
            outboxConnection.Transaction.Dispose();
            outboxConnection.Connection.Dispose();
        });

        await next();
    }
}