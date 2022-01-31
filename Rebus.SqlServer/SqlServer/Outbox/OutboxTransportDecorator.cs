using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Rebus.Bus;
using Rebus.Logging;
using Rebus.Messages;
using Rebus.Pipeline;
using Rebus.Threading;
using Rebus.Transport;

namespace Rebus.SqlServer.Outbox;

class OutboxTransportDecorator : ITransport, IInitializable, IDisposable
{
    public const string BypassOutboxKey = "rebus-bypass-outbox";

    static readonly Retrier SendRetrier = new(new[]
    {
        TimeSpan.FromSeconds(0.1),
        TimeSpan.FromSeconds(0.1),
        TimeSpan.FromSeconds(0.1),
        TimeSpan.FromSeconds(0.1),
        TimeSpan.FromSeconds(0.1),
        TimeSpan.FromSeconds(0.5),
        TimeSpan.FromSeconds(0.5),
        TimeSpan.FromSeconds(0.5),
        TimeSpan.FromSeconds(0.5),
        TimeSpan.FromSeconds(0.5),
        TimeSpan.FromSeconds(1),
        TimeSpan.FromSeconds(1),
        TimeSpan.FromSeconds(1),
        TimeSpan.FromSeconds(1),
        TimeSpan.FromSeconds(1),
    });

    readonly CancellationTokenSource _cancellationTokenSource = new();
    readonly IOutboxStorage _outboxStorage;
    readonly ITransport _transport;
    readonly ILog _logger;

    readonly IAsyncTask _forwarder;
    readonly IAsyncTask _cleaner;

    public OutboxTransportDecorator(IRebusLoggerFactory rebusLoggerFactory, ITransport transport, IOutboxStorage outboxStorage, IAsyncTaskFactory asyncTaskFactory)
    {
        _transport = transport;
        _outboxStorage = outboxStorage;
        _logger = rebusLoggerFactory.GetLogger<OutboxTransportDecorator>();
        _forwarder = asyncTaskFactory.Create("OutboxForwarder", RunForwarder, intervalSeconds: 1);
        _cleaner = asyncTaskFactory.Create("OutboxCleaner", RunCleaner, intervalSeconds: 120);
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
#pragma warning disable CS4014
            Task CompletedAction(ITransactionContext _)
            {
                if (!queue.Any()) return Task.CompletedTask;

                Task.Run(async () =>
                {
                    try
                    {
                        using var batch = await _outboxStorage.GetNextMessageBatch(correlationId);

                        await ProcessMessageBatch(batch, _cancellationTokenSource.Token);

                        await batch.Complete();
                    }
                    catch (Exception)
                    {
                        // just leave sending to the background sender
                    }
                });

                return Task.CompletedTask;
            }

            context.OnCompleted(CompletedAction);
#pragma warning restore CS4014

            return queue;
        }

        var outgoingMessages = context.GetOrAdd("rebus-outbox-messages", SendOutgoingMessages);

        outgoingMessages.Enqueue(new AbstractRebusTransport.OutgoingMessage(message, destinationAddress));
    }

    public Task<TransportMessage> Receive(ITransactionContext context, CancellationToken cancellationToken) => _transport.Receive(context, cancellationToken);

    public string Address => _transport.Address;

    public void Initialize()
    {
        _forwarder.Start();
        _cleaner.Start();
    }

    public void Dispose()
    {
        _cancellationTokenSource.Cancel();

        _cleaner.Dispose();
        _forwarder.Dispose();

        _cancellationTokenSource.Dispose();
    }

    async Task RunForwarder()
    {
        _logger.Debug("Checking outbox storage for pending messages");

        var cancellationToken = _cancellationTokenSource.Token;

        while (!cancellationToken.IsCancellationRequested)
        {
            using var batch = await _outboxStorage.GetNextMessageBatch();

            if (!batch.Any())
            {
                _logger.Debug("No pending messages found");
                return;
            }

            await ProcessMessageBatch(batch, cancellationToken);

            await batch.Complete();
        }
    }

    async Task ProcessMessageBatch(IReadOnlyCollection<OutboxMessage> batch, CancellationToken cancellationToken)
    {
        _logger.Debug("Sending {count} pending messages", batch.Count);

        using var scope = new RebusTransactionScope();

        foreach (var message in batch)
        {
            var destinationAddress = message.DestinationAddress;
            var transportMessage = message.ToTransportMessage();
            var transactionContext = scope.TransactionContext;

            Task SendMessage() => _transport.Send(destinationAddress, transportMessage, transactionContext);

            await SendRetrier.ExecuteAsync(SendMessage, cancellationToken);
        }

        await scope.CompleteAsync();

        _logger.Debug("Successfully sent {count} messages", batch.Count);
    }

    async Task RunCleaner()
    {
        _logger.Debug("Checking outbox storage for messages to be deleted");
    }
}
