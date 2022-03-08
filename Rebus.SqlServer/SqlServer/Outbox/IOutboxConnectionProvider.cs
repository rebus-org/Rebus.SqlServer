namespace Rebus.SqlServer.Outbox;

interface IOutboxConnectionProvider
{
    IDbConnection GetDbConnection();
}