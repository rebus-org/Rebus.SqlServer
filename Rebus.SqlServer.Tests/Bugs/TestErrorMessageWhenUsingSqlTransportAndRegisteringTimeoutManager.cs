using System;
using NUnit.Framework;
using Rebus.Activation;
using Rebus.Config;

namespace Rebus.SqlServer.Tests.Bugs
{
    [TestFixture]
    public class TestErrorMessageWhenUsingSqlTransportAndRegisteringTimeoutManager
    {
        [Test]
        public void PrintException()
        {
            try
            {
                Configure.With(new BuiltinHandlerActivator())
                    .Transport(t => t.UseSqlServer(new SqlServerTransportOptions(SqlTestHelper.ConnectionString), "whatever"))
                    .Timeouts(t => t.StoreInSqlServer(SqlTestHelper.ConnectionString, "timeouts"))
                    .Start();
            }
            catch (Exception exception)
            {
                Console.WriteLine(exception);
            }
        }
    }
}
