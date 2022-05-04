using System;
using System.Threading;
using System.Threading.Tasks;
using Rebus.Messages;
using Rebus.Transport;

namespace Rebus.SqlServer.Tests.Outbox;

class FlakySenderTransportDecorator : ITransport
{
    readonly ITransport _transport;
    readonly FlakySenderTransportDecoratorSettings _flakySenderTransportDecoratorSettings;

    public FlakySenderTransportDecorator(ITransport transport,
        FlakySenderTransportDecoratorSettings flakySenderTransportDecoratorSettings)
    {
        _transport = transport;
        _flakySenderTransportDecoratorSettings = flakySenderTransportDecoratorSettings;
    }

    public void CreateQueue(string address) => _transport.CreateQueue(address);

    public Task Send(string destinationAddress, TransportMessage message, ITransactionContext context)
    {
        if (Random.Shared.NextDouble() > _flakySenderTransportDecoratorSettings.SuccessRate)
        {
            throw new RandomUnluckyException();
        }

        return _transport.Send(destinationAddress, message, context);
    }

    public Task<TransportMessage> Receive(ITransactionContext context, CancellationToken cancellationToken)
    {
        return _transport.Receive(context, cancellationToken);
    }

    public string Address => _transport.Address;
}