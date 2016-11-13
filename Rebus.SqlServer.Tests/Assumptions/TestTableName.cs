using NUnit.Framework;

namespace Rebus.SqlServer.Tests.Assumptions
{
    [TestFixture]
    public class TestTableName
    {
        [Test]
        public void ParsesNameWithoutSchemaAssumingDboAsDefault()
        {
            var table = TableName.Parse("TableName");

            Assert.AreEqual(table.Name, "TableName");
            Assert.AreEqual(table.Schema, "dbo");
            Assert.AreEqual(table.QualifiedName, "[dbo].[TableName]");
        }

        [Test]
        public void ParsesBracketsNameWithoutSchemaAssumingDboAsDefault()
        {
            var table = TableName.Parse("[TableName]");

            Assert.AreEqual(table.Name, "TableName");
            Assert.AreEqual(table.Schema, "dbo");
            Assert.AreEqual(table.QualifiedName, "[dbo].[TableName]");
        }

        [Test]
        public void ParsesNameWithSchema()
        {
            var table = TableName.Parse("schema.TableName");

            Assert.AreEqual(table.Name, "TableName");
            Assert.AreEqual(table.Schema, "schema");
            Assert.AreEqual(table.QualifiedName, "[schema].[TableName]");
        }

        [Test]
        public void ParsesBracketsNameWithSchema()
        {
            var table = TableName.Parse("[schema].[TableName]");

            Assert.AreEqual(table.Name, "TableName");
            Assert.AreEqual(table.Schema, "schema");
            Assert.AreEqual(table.QualifiedName, "[schema].[TableName]");
        }
    }
}
