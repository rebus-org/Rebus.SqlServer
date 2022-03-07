using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Rebus.Messages;

namespace Rebus.SqlServer.Outbox;

/// <summary>
/// Wraps a batch of <see cref="OutboxMessage"/>s along with a function that "completes" the batch (i.e. ensures that it will not be handled again -... e.g. by deleting it, or marking it as completed)
/// </summary>
public class OutboxMessageBatch : IDisposable, IReadOnlyList<OutboxMessage>
{
    /// <summary>
    /// Gets an empty outbox message batch that doesn't complete anything and only performs some kind of cleanup when done
    /// </summary>
    public static OutboxMessageBatch Empty(Action disposeFunction) => new(() => Task.CompletedTask, Array.Empty<OutboxMessage>(), disposeFunction);

    readonly IReadOnlyList<OutboxMessage> _messages;
    readonly Func<Task> _completionFunction;
    readonly Action _disposeFunction;

    /// <summary>
    /// Creates the batch
    /// </summary>
    public OutboxMessageBatch(Func<Task> completionFunction, IEnumerable<OutboxMessage> messages, Action disposeFunction)
    {
        _messages = messages.ToList();
        _completionFunction = completionFunction ?? throw new ArgumentNullException(nameof(completionFunction));
        _disposeFunction = disposeFunction;
    }

    /// <summary>
    /// Marks the message batch as properly handled
    /// </summary>
    public async Task Complete() => await _completionFunction();

    /// <summary>
    /// Performs any cleanup actions necessary
    /// </summary>
    public void Dispose() => _disposeFunction();

    /// <summary>
    /// Gets how many
    /// </summary>
    public int Count => _messages.Count;

    /// <summary>
    /// Gets by index
    /// </summary>
    public OutboxMessage this[int index] => _messages[index];

    /// <summary>
    /// Gets an enumerator for the wrapped sequence of <see cref="OutboxMessage"/>s
    /// </summary>
    public IEnumerator<OutboxMessage> GetEnumerator() => _messages.GetEnumerator();

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
}

/// <summary>
/// Represents one single message to be delivered to the transport
/// </summary>
public record OutboxMessage(long Id, string DestinationAddress, Dictionary<string, string> Headers, byte[] Body)
{
    /// <summary>
    /// Gets the <see cref="Headers"/> and <see cref="Body"/> wrapped in a <see cref="TransportMessage"/>
    /// </summary>
    public TransportMessage ToTransportMessage() => new(Headers, Body);
}
