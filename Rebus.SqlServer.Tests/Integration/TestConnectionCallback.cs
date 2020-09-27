using System;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using Rebus.Activation;
using Rebus.Config;
using Rebus.Logging;
using Rebus.Tests.Contracts;
using Rebus.Tests.Contracts.Extensions;

#pragma warning disable 1998

namespace Rebus.SqlServer.Tests.Integration
{
    [TestFixture]
    public class TestConnectionCallback : FixtureBase
    {
        [Test]
        public async Task SeeIfItWorks()
        {
            var callBackWasCalledAlright = Using(new ManualResetEvent(initialState: false));
            var activator = Using(new BuiltinHandlerActivator());

            Configure.With(activator)
                .Transport(t =>
                {
                    var options = new SqlServerTransportOptions(c =>
                    {
                        var connectionString = SqlTestHelper.ConnectionString;
                        var loggerFactory = c.Get<IRebusLoggerFactory>();

                        var connectionProvider = new DbConnectionProvider(connectionString, loggerFactory)
                        {
                            SqlConnectionOpening = async connection => callBackWasCalledAlright.Set()
                        };

                        return connectionProvider;
                    });
                    t.UseSqlServer(options, "test-queue");
                })
                .Start();

            callBackWasCalledAlright.WaitOrDie(
                timeout: TimeSpan.FromSeconds(3),
                errorMessage: $"The '{nameof(DbConnectionProvider.SqlConnectionOpening)}' callback was not invoked as expected"
            );
        }
    }
}
