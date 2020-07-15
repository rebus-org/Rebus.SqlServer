using System;
using System.Linq;
using System.Threading.Tasks;

using Microsoft.Data.SqlClient;

using Rebus.Logging;

using IsolationLevel = System.Data.IsolationLevel;

#pragma warning disable 1998

namespace Rebus.SqlServer
{
    /// <summary>
    /// Implementation of <see cref="IDbConnectionProvider"/> that ensures that MARS (multiple active result sets) is enabled on the
    /// given connection string (possibly by enabling it by itself)
    /// </summary>
    public class DbConnectionProvider : IDbConnectionProvider
    {
        readonly bool _enlistInAmbientTransaction;
        readonly string _connectionString;
        readonly ILog _log;

        /// <summary>
        /// Wraps the connection string with the given name from app.config (if it is found), or interprets the given string as
        /// a connection string to use. Will use <see cref="System.Data.IsolationLevel.ReadCommitted"/> by default on transactions,
        /// unless another isolation level is set with the <see cref="IsolationLevel"/> property
        /// </summary>
        public DbConnectionProvider(string connectionString, IRebusLoggerFactory rebusLoggerFactory
            , bool enlistInAmbientTransaction = false
            )
        {
            if (rebusLoggerFactory == null) throw new ArgumentNullException(nameof(rebusLoggerFactory));

            _log = rebusLoggerFactory.GetLogger<DbConnectionProvider>();

            _connectionString = EnsureMarsIsEnabled(connectionString);
            _enlistInAmbientTransaction = enlistInAmbientTransaction;
            IsolationLevel = IsolationLevel.ReadCommitted;
        }

        string EnsureMarsIsEnabled(string connectionString)
        {
            var connectionStringSettings = connectionString.Split(new[] { ";" }, StringSplitOptions.RemoveEmptyEntries)
                .Select(kvp => kvp.Split(new[] { "=" }, StringSplitOptions.RemoveEmptyEntries))
                .ToDictionary(kvp => kvp[0], kvp => string.Join("=", kvp.Skip(1)), StringComparer.OrdinalIgnoreCase);

            if (!connectionStringSettings.ContainsKey("MultipleActiveResultSets"))
            {
                _log.Info("Supplied connection string will be modified to enable MARS");

                connectionStringSettings["MultipleActiveResultSets"] = "True";
            }

            return string.Join("; ", connectionStringSettings.Select(kvp => $"{kvp.Key}={kvp.Value}"));
        }

        /// <summary>
        /// Gets a nice ready-to-use database connection with an open transaction
        /// </summary>
        public async Task<IDbConnection> GetConnection()
        {
            SqlConnection connection = null;
            SqlTransaction transaction = null;
            try
            {
                if (_enlistInAmbientTransaction == false)
                {
                    connection = CreateSqlConnectionSuppressingAPossibleAmbientTransaction();
                    transaction = connection.BeginTransaction(IsolationLevel);
                }
                else
                {
                    connection = CreateSqlConnectionInAPossiblyAmbientTransaction();
                }

                return new DbConnectionWrapper(connection, transaction, false);
            }
            catch (Exception)
            {
                connection?.Dispose();
                throw;
            }
        }

        SqlConnection CreateSqlConnectionInAPossiblyAmbientTransaction()
        {
            var connection = new SqlConnection(_connectionString);

            // do not use Async here! it would cause the tx scope to be disposed on another thread than the one that created it
            connection.Open();

            var transaction = System.Transactions.Transaction.Current;
            if (transaction != null)
            {
                connection.EnlistTransaction(transaction);
            }

            return connection;
        }

        SqlConnection CreateSqlConnectionSuppressingAPossibleAmbientTransaction()
        {
            SqlConnection connection;
            using (new System.Transactions.TransactionScope(System.Transactions.TransactionScopeOption.Suppress))
            {
                connection = new SqlConnection(_connectionString);

                // do not use Async here! it would cause the tx scope to be disposed on another thread than the one that created it
                connection.Open();
            }

            return connection;
        }

        /// <summary>
        /// Gets/sets the isolation level used for transactions
        /// </summary>
        public IsolationLevel IsolationLevel { get; set; }
    }
}
