using System;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using Rebus.Activation;
using Rebus.Config;
using Rebus.Tests.Contracts;
using Rebus.Tests.Contracts.Extensions;
using Rebus.Timeouts;
#pragma warning disable 1998

namespace Rebus.SqlServer.Tests.Bugs
{
    [TestFixture]
    public class TestSqlTransportAndDedicatedTimeoutManager : FixtureBase
    {
        protected override void SetUp()
        {
            base.SetUp();

            SqlTestHelper.DropAllTables();

            Using(new DisposableCallback(SqlTestHelper.DropAllTables));
        }

        [Test]
        public async Task ItWorks()
        {
            using var gotTheString = new ManualResetEvent(initialState: false);

            var connectionString = SqlTestHelper.ConnectionString;

            using var timeoutManager = new BuiltinHandlerActivator();

            Configure.With(timeoutManager)
                .Transport(t => t.UseSqlServer(new SqlServerTransportOptions(connectionString), "TimeoutManager").DisableNativeTimeoutManager())
                .Timeouts(t => t.StoreInSqlServer(connectionString, "Timeouts"))
                .Start();

            using var ordinaryEndpoint = new BuiltinHandlerActivator();

            ordinaryEndpoint.Handle<string>(async str => gotTheString.Set());

            Configure.With(ordinaryEndpoint)
                .Transport(t => t.UseSqlServer(new SqlServerTransportOptions(connectionString), "OrdinaryEndpoint").DisableNativeTimeoutManager())
                .Timeouts(t => t.UseExternalTimeoutManager("TimeoutManager"))
                .Start();

            await ordinaryEndpoint.Bus.DeferLocal(TimeSpan.FromSeconds(2), "HEJ MED DIG MIN VEN 🤠");

            gotTheString.WaitOrDie(timeout: TimeSpan.FromSeconds(5), errorMessage: "Did not receive the expected string message within 5 s timeout");
        }
    }
}
