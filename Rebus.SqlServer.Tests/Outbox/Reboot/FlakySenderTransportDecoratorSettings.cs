namespace Rebus.SqlServer.Tests.Outbox.Reboot;

class FlakySenderTransportDecoratorSettings
{
    public double SuccessRate { get; set; } = 1;
}