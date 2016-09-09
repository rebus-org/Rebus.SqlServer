using NUnit.Framework;
using Rebus.Tests.Contracts.Subscriptions;

namespace Rebus.SqlServer.Tests.Subscriptions
{
    [TestFixture, Category(Categories.SqlServer)]
    public class SqlServerSubscriptionStorageBasicSubscriptionOperations : BasicSubscriptionOperations<SqlServerSubscriptionStorageFactory>
    {
    }
}