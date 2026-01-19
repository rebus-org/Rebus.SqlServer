using System.Threading.Tasks;
using NUnit.Framework;
using Rebus.Activation;
using Rebus.Config;
using Rebus.Serialization.Json;
using Rebus.Tests.Contracts;

namespace Rebus.SqlServer.Tests.Bugs;

[TestFixture]
[Description("Trying to reproduce an error reported by a used. Currently cannot reproduce.")]
public class ReplicateStartupError : FixtureBase
{
    [Test]
    public async Task CanDoIt()
    {
        var connectionString = SqlTestHelper.ConnectionString;

        using var adapter = new BuiltinHandlerActivator();

        using var bus = Configure.With(adapter)
            .Logging(l => l.ColoredConsole())
            .Transport(t => t.UseSqlServer(new SqlServerTransportOptions(connectionString), "messages"))
            .Subscriptions(s => s.StoreInSqlServer(connectionString, "consumers", automaticallyCreateTables: true))
            .Serialization(s => s.UseSystemTextJson())
            .Start();

        await Task.Delay(millisecondsDelay: 200);
    }
}