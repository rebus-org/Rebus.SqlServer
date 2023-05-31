using System.Threading.Tasks;

namespace Rebus.SqlServer.Outbox;

interface IOutboxConnectionProvider
{
    Task<OutboxConnection> GetDbConnection();
}