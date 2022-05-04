namespace Rebus.SqlServer.Tests.Outbox;

class FlakySenderTransportDecoratorSettings
{
    public double SuccessRate { get; set; } = 1;
}