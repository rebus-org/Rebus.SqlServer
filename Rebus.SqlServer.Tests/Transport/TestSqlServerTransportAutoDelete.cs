using System.Linq;
using System.Threading.Tasks;
using NUnit.Framework;
using Rebus.Activation;
using Rebus.Config;
using Rebus.Logging;
using Rebus.Tests.Contracts;

namespace Rebus.SqlServer.Tests.Transport
{
	[TestFixture, Category(Categories.SqlServer)]
	public class TestSqlServerTransportAutoDelete : FixtureBase
    {
        protected override void SetUp()
        {
            SqlTestHelper.DropAllTables();
        }

        [Test]
        public async Task Dispose_WhenAutoDeleteQueueEnabled_DropsInputQueue()
        {
            var consoleLoggerFactory = new ConsoleLoggerFactory(false);
            var connectionProvider = new DbConnectionProvider(SqlTestHelper.ConnectionString, consoleLoggerFactory);

            const string queueName = "input";
            
            var options = new SqlServerTransportOptions(SqlTestHelper.ConnectionString);

            var activator = Using(new BuiltinHandlerActivator());
            Configure.With(activator)
                .Logging(l => l.Use(consoleLoggerFactory))
                .Transport(t => t.UseSqlServer(options, queueName).SetAutoDeleteQueue(true))
                .Start();

            using (var connection = await connectionProvider.GetConnection())
            {
                Assert.That(connection.GetTableNames().Contains(TableName.Parse(queueName)), Is.True);
            }

            CleanUpDisposables();

            using (var connection = await connectionProvider.GetConnection())
            {
                Assert.That(connection.GetTableNames().Contains(TableName.Parse(queueName)), Is.False);
            }
        }
    }
}
