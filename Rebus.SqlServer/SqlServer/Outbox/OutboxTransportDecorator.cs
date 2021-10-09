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

namespace Rebus.SqlServer.Outbox
{
    class OutboxTransportDecorator : ITransport, IInitializable, IDisposable
    {
        public const string OutboxEnabledKey = "rebus-outbox-enabled";

        private readonly CancellationTokenSource _cancellationTokenSource = new();
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
            if (!context.Items.ContainsKey(OutboxEnabledKey))
            {
                await _transport.Send(destinationAddress, message, context);
                return;
            }

            var outgoingMessages = context.GetOrAdd("outbox-messages", () =>
            {
                var queue = new ConcurrentQueue<AbstractRebusTransport.OutgoingMessage>();

                // if we're currently handling a message, we get information about it here
                if (context.Items.TryGetValue("stepContext", out var result) && result is IncomingStepContext stepContext)
                {
                    var messageId = stepContext.Load<TransportMessage>().GetMessageId();
                    var sourceQueue = _transport.Address;

                    context.OnCommitted(async _ => await _outboxStorage.Save(messageId, sourceQueue, queue));
                }
                else
                {
                    context.OnCommitted(async _ => await _outboxStorage.Save(queue));
                }

                context.OnCompleted(async => Task.Run(async () =>
                {

                }));

                return queue;
            });

            outgoingMessages.Enqueue(new AbstractRebusTransport.OutgoingMessage(message, destinationAddress));

            //await _transport.Send(destinationAddress, message, context);
        }

        public async Task<TransportMessage> Receive(ITransactionContext context, CancellationToken cancellationToken)
        {
            return await _transport.Receive(context, cancellationToken);
        }

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

            while (!_cancellationTokenSource.IsCancellationRequested)
            {
                using var batch = await _outboxStorage.GetNextMessageBatch();

                if (!batch.Any())
                {
                    _logger.Debug("No pending messages found");
                    return;
                }

                _logger.Debug("Sending {count} pending messages", batch.Count());

                using var scope = new RebusTransactionScope();

                foreach (var message in batch)
                {
                    await _transport.Send(message.DestinationAddress, message.ToTransportMessage(), scope.TransactionContext);
                }

                await scope.CompleteAsync();
                await batch.Complete();

                _logger.Debug("Successfully sent {count} messages", batch.Count());
            }
        }

        async Task RunCleaner()
        {
            _logger.Debug("Checking outbox storage for messages to be deleted");
        }
    }
}
