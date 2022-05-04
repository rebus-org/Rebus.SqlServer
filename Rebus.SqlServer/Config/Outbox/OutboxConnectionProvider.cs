using System;
using Microsoft.Data.SqlClient;
using Rebus.SqlServer.Outbox;

namespace Rebus.Config.Outbox;

class OutboxConnectionProvider : IOutboxConnectionProvider
{
    readonly string _connectionString;

    public OutboxConnectionProvider(string connectionString)
    {
        _connectionString = connectionString ?? throw new ArgumentNullException(nameof(connectionString));
    }

    public OutboxConnection GetDbConnection()
    {
        var connection = new SqlConnection(_connectionString);

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