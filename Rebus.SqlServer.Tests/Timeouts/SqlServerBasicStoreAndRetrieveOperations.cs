using NUnit.Framework;
using Rebus.Tests.Contracts.Timeouts;

namespace Rebus.SqlServer.Tests.Timeouts
{
    [TestFixture, Category(Categories.SqlServer)]
    public class SqlServerBasicStoreAndRetrieveOperations : BasicStoreAndRetrieveOperations<SqlServerTimeoutManagerFactory>
    {
    }
}