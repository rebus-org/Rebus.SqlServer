using System;
using Microsoft.Data.SqlClient;

namespace Rebus.SqlServer.Outbox;

/// <summary>
/// Holds an open <see cref="SqlConnection"/>
/// </summary>
public class OutboxConnection
{
    /// <summary>
    /// Gets the connection
    /// </summary>
    public SqlConnection Connection { get; }
    
    /// <summary>
    /// Gets the current transaction
    /// </summary>
    public SqlTransaction Transaction { get; }

    internal OutboxConnection(SqlConnection connection, SqlTransaction transaction)
    {
        Connection = connection ?? throw new ArgumentNullException(nameof(connection));
        Transaction = transaction ?? throw new ArgumentNullException(nameof(transaction));
    }
}
