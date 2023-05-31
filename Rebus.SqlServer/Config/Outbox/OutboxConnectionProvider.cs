using System;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Rebus.SqlServer;
using Rebus.SqlServer.Outbox;
#pragma warning disable CS1998

namespace Rebus.Config.Outbox;

class OutboxConnectionProvider : IOutboxConnectionProvider
{
    readonly Func<Task<OutboxConnection>> _factory;

    public OutboxConnectionProvider(string connectionString)
    {
        if (connectionString == null) throw new ArgumentNullException(nameof(connectionString));

        _factory = async () => GetOutboxConnectionFromConnectionString(connectionString);
    }

    public OutboxConnectionProvider(Func<Task<IDbConnection>> connectionFactory)
    {
        if (connectionFactory == null) throw new ArgumentNullException(nameof(connectionFactory));

        _factory = async () =>
        {
            var connection = await connectionFactory();
            return null;
            //return new OutboxConnection(connection.)
        };
    }

    public Task<OutboxConnection> GetDbConnection() => _factory();

    static OutboxConnection GetOutboxConnectionFromConnectionString(string connectionString)
    {
        var connection = new SqlConnection(connectionString);

        try
        {
            connection.Open();

            var transaction = connection.BeginTransaction();

            return new OutboxConnection(connection, transaction);
        }
        catch
        {
            connection.Dispose();
            throw;
        }
    }

}