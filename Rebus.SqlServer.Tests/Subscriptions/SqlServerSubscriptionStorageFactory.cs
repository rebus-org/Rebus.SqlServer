using Rebus.Logging;
using Rebus.SqlServer.Subscriptions;
using Rebus.Subscriptions;
using Rebus.Tests.Contracts.Subscriptions;

namespace Rebus.SqlServer.Tests.Subscriptions
{
    public class SqlServerSubscriptionStorageFactory : ISubscriptionStorageFactory
    {
        const string TableName = "RebusSubscriptions";
        
        public ISubscriptionStorage Create()
        {
            var consoleLoggerFactory = new ConsoleLoggerFactory(true);
            var connectionProvider = new DbConnectionProvider(SqlTestHelper.ConnectionString, consoleLoggerFactory);
            var storage = new SqlServerSubscriptionStorage(connectionProvider, TableName, true, consoleLoggerFactory);

            storage.EnsureTableIsCreated();
            
            return storage;
        }

        public void Cleanup()
        {
            SqlTestHelper.DropTable(TableName);
        }
    }
}