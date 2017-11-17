using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Threading.Tasks;
using NUnit.Framework;
using Rebus.Activation;
using Rebus.Config;
using Rebus.Logging;
using Rebus.SqlServer.Transport;
using Rebus.Tests.Contracts;
using Rebus.Tests.Contracts.Utilities;
#pragma warning disable 1998

namespace Rebus.SqlServer.Tests.Integration
{
    [TestFixture]
    [Ignore("Can't run schema migration with snapshot isolation level")]
    public class SnapshotIsolationLevel : FixtureBase
    {
        const string TableName = "Messages";

        protected override void SetUp()
        {
            SqlTestHelper.DropTable(TableName);

            SqlTestHelper.Execute(@"

CREATE TABLE [dbo].[Messages](
	[id] [bigint] IDENTITY(1,1) NOT NULL,
	[recipient] [nvarchar](200) NOT NULL,
	[priority] [int] NOT NULL,
	[expiration] [datetime2](7) NOT NULL,
	[visible] [datetime2](7) NOT NULL,
	[headers] [varbinary](max) NOT NULL,
	[body] [varbinary](max) NOT NULL,
	CONSTRAINT [PK_Messages] PRIMARY KEY CLUSTERED 
	(
		[recipient] ASC,
		[priority] ASC,
		[id] ASC
	)
)

");

        }

        protected override void TearDown()
        {
        }

        [Test]
        public async Task ItWorks()
        {
            var activator = new BuiltinHandlerActivator();

            Using(activator);

            const int messageCount = 200;

            var counter = new SharedCounter(messageCount);

            activator.Handle<string>(async str =>
            {
                counter.Decrement();
            });

            Configure.With(activator)
                .Logging(l => l.Console(LogLevel.Info))
                .Transport(t =>
                {
                    var connectionString = SqlTestHelper.ConnectionString;

                    t.UseSqlServer(async () =>
                    {
                        var sqlConnection = new SqlConnection(connectionString);
                        await sqlConnection.OpenAsync();
                        var transaction = sqlConnection.BeginTransaction(IsolationLevel.Snapshot);
                        return new DbConnectionWrapper(sqlConnection, transaction, false);
                    }, "snapperino");
                })
                .Start();

            await Task.WhenAll(Enumerable.Range(0, messageCount)
                .Select(i => activator.Bus.SendLocal($"MAKE {i} GREAT AGAIN!!!!")));

            counter.WaitForResetEvent();
        }
    }
} 