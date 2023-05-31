using System;
using System.Threading.Tasks;
using Rebus.Config.Outbox;
using Rebus.Pipeline;
using Rebus.Transport;
#pragma warning disable CS1998

namespace Rebus.SqlServer.Outbox;

class OutboxIncomingStep : IIncomingStep
{
    readonly IOutboxConnectionProvider _outboxConnectionProvider;

    public OutboxIncomingStep(IOutboxConnectionProvider outboxConnectionProvider)
    {
        _outboxConnectionProvider = outboxConnectionProvider ?? throw new ArgumentNullException(nameof(outboxConnectionProvider));
    }

    public async Task Process(IncomingStepContext context, Func<Task> next)
    {
        var outboxConnection = await _outboxConnectionProvider.GetDbConnection();
        var transactionContext = context.Load<ITransactionContext>();

        transactionContext.Items[OutboxExtensions.CurrentOutboxConnectionKey] = outboxConnection;

        transactionContext.OnCommit(async _ => outboxConnection.Transaction.Commit());

        transactionContext.OnDisposed(_ =>
        {
            outboxConnection.Transaction.Dispose();
            outboxConnection.Connection.Dispose();
        });

        await next();
    }
}