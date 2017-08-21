using NUnit.Framework;
using Rebus.SqlServer.Tests.Transport.Contract.Factories;
using Rebus.Tests.Contracts.Transports;

namespace Rebus.SqlServer.Tests.Transport.Contract
{
    [TestFixture]
    public class SqlServerLeaseTestManyMessages : TestManyMessages<SqlServerLeaseBusFactory> { }
}