namespace Rebus.SqlServer.Outbox;

interface IOutboxConnectionProvider
{
    OutboxConnection GetDbConnection();
}