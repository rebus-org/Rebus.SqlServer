using NUnit.Framework;
using Rebus.Tests.Contracts.Transports;

namespace Rebus.SqlServer.Tests.Transport
{
    [TestFixture]
    public class SqlServerTestManyMessages : TestManyMessages<SqlServerBusFactory> { }
}