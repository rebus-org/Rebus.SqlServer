using System;
using Rebus.SqlServer.Outbox;
using Rebus.Transport;

namespace Rebus.Config.Outbox;

/// <summary>
/// Extensions for enabling Rebus' outbox
/// </summary>
public static class RebusTransactionScopeExtensions
{
    /// <summary>
    /// Adds a key to the scope's transaction context's <see cref="ITransactionContext.Items"/> collection, signaling
    /// that subsequent send/publish operations with the scope should NOT be enlisted in the current database transaction by using Rebus' outbox.
    /// </summary>
    public static void BypassOutbox(this RebusTransactionScope scope)
    {
        if (scope == null)
        {
            throw new ArgumentNullException(nameof(scope));
        }

        var transactionContext = scope.TransactionContext;

        transactionContext.Items[OutboxTransportDecorator.BypassOutboxKey] = "";
    }
}
