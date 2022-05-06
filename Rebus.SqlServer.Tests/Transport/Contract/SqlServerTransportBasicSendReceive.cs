using NUnit.Framework;
using Rebus.SqlServer.Tests.Transport.Contract.Factories;
using Rebus.Tests.Contracts.Transports;

namespace Rebus.SqlServer.Tests.Transport.Contract;

[TestFixture, Category(Categories.SqlServer)]
public class SqlServerTransportBasicSendReceive : BasicSendReceive<SqlTransportFactory>
{
    protected override TransportBehavior Behavior => new(ReturnsNullWhenQueueIsEmpty: true);
}