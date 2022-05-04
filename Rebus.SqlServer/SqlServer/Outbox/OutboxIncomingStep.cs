using System;
using System.Threading.Tasks;
using Rebus.Config.Outbox;
using Rebus.Pipeline;
using Rebus.Transport;

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
        var dbConnection = _outboxConnectionProvider.GetDbConnection();
        var transactionContext = context.Load<ITransactionContext>();

        transactionContext.Items[OutboxExtensions.CurrentOutboxConnectionKey] = dbConnection;
    }
}