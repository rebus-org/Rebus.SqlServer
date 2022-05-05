using System;
using System.Threading;
using System.Threading.Tasks;
using Rebus.Messages;
using Rebus.Transport;
// ReSharper disable AccessToModifiedClosure
#pragma warning disable CS1998

namespace Rebus.SqlServer.Tests.Outbox;

class FlakyAckerTransportDecorator : ITransport
{
    readonly ITransport _transport;

    int _ackFailures;

    public FlakyAckerTransportDecorator(ITransport transport, int ackFailures)
    {
        _transport = transport ?? throw new ArgumentNullException(nameof(transport));
        _ackFailures = ackFailures;
    }

    public void CreateQueue(string address) => _transport.CreateQueue(address);

    public Task Send(string destinationAddress, TransportMessage message, ITransactionContext context) => _transport.Send(destinationAddress, message, context);

    public async Task<TransportMessage> Receive(ITransactionContext context, CancellationToken cancellationToken)
    {
        TransportMessage transportMessage = null;

        // be sure to register this completion handler first
        async Task CompletedAction(ITransactionContext _)
        {
            // if no message was received, don't do anything
            if (transportMessage == null) return;

            // only at this point can we know whether we should throw
            var throwException = Interlocked.Decrement(ref _ackFailures) + 1 >= 0;

            // don't do anything 
            if (!throwException) return;

            Console.WriteLine("THROWING ACK FAILURE");

            throw new AckFailureException("oh no could not ACK");
        }

        context.OnCompleted(CompletedAction);

        transportMessage = await _transport.Receive(context, cancellationToken);

        return transportMessage;
    }

    public string Address => _transport.Address;
}

class AckFailureException : Exception
{
    public AckFailureException(string message) : base(message)
    {
    }
}