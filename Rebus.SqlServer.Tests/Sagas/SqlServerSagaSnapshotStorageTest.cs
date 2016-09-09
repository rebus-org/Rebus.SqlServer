using NUnit.Framework;
using Rebus.Tests.Contracts.Sagas;

namespace Rebus.SqlServer.Tests.Sagas
{
    [TestFixture]
    public class SqlServerSagaSnapshotStorageTest : SagaSnapshotStorageTest<SqlServerSnapshotStorageFactory>
    {
    }
}