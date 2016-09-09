using NUnit.Framework;
using Rebus.Tests.Contracts.DataBus;

namespace Rebus.SqlServer.Tests.DataBus
{
    [TestFixture]
    public class SqlServerDataBusStorageTest : GeneralDataBusStorageTests<SqlServerDataBusStorageFactory> { }
}