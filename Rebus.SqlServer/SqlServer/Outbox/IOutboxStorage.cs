using System.Collections.Generic;
using System.Threading.Tasks;
using Rebus.Transport;

namespace Rebus.SqlServer.Outbox;

/// <summary>
/// Outbox abstraction that enables truly idempotent message processing and store-and-forward for outgoing messages
/// </summary>
public interface IOutboxStorage
{
    /// <summary>
    /// Stores the given <paramref name="outgoingMessages"/> as being the result of processing message with ID <paramref name="messageId"/>
    /// in the queue of this particular endpoint. If <paramref name="outgoingMessages"/> is an empty sequence, a note is made of the fact
    /// that the message with ID <paramref name="messageId"/> has been processed.
    /// </summary>
    Task Save(IEnumerable<AbstractRebusTransport.OutgoingMessage> outgoingMessages, string messageId = null, string sourceQueue = null, string correlationId = null);

    /// <summary>
    /// Stores the given <paramref name="outgoingMessages"/> using the given <paramref name="dbConnection"/>.
    /// </summary>
    Task Save(IEnumerable<AbstractRebusTransport.OutgoingMessage> outgoingMessages, IDbConnection dbConnection);

    /// <summary>
    /// Gets the next message batch to be sent, possibly filtered by the given <paramref name="correlationId"/>. MIGHT return messages from other send operations in the rare
    /// case where there is a colission between correlation IDs. Returns from 0 to <paramref name="maxMessageBatchSize"/> messages in the batch.
    /// </summary>
    Task<OutboxMessageBatch> GetNextMessageBatch(string correlationId = null, int maxMessageBatchSize = 100);
}