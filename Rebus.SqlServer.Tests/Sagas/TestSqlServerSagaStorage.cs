using System;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using NUnit.Framework;
using Rebus.Logging;
using Rebus.Pipeline;
using Rebus.Sagas;
using Rebus.SqlServer.Sagas;
using Rebus.SqlServer.Sagas.Serialization;
using Rebus.Tests.Contracts;
using Rebus.Tests.Contracts.Sagas;
using Rebus.Transport;

namespace Rebus.SqlServer.Tests.Sagas;

[TestFixture]
public class TestSqlServerSagaStorage : FixtureBase
{
    SqlServerSagaStorage _storage;
    string _dataTableName;
    DbConnectionProvider _connectionProvider;
    string _indexTableName;

    protected override void SetUp()
    {
        var loggerFactory = new ConsoleLoggerFactory(false);
        _connectionProvider = new DbConnectionProvider(SqlTestHelper.ConnectionString, loggerFactory);
        var sagaTypeNamingStrategy = new LegacySagaTypeNamingStrategy();
        var serializer = new DefaultSagaSerializer();

        _dataTableName = TestConfig.GetName("sagas");
        _indexTableName = TestConfig.GetName("sagaindex");

        SqlTestHelper.DropTable(_indexTableName);
        SqlTestHelper.DropTable(_dataTableName);

        _storage = new SqlServerSagaStorage(_connectionProvider, _dataTableName, _indexTableName, loggerFactory, sagaTypeNamingStrategy, serializer);
    }

    [TestCase(10)]
    [TestCase(100)]
    [TestCase(1000)]
    [TestCase(10000)]
    public async Task CheckPerformanceOfRapidUpdates(int iterations)
    {
        _storage.Initialize();
        _storage.EnsureTablesAreCreated();

        var sagaData = new MySagaDizzle
        {
            Id = Guid.NewGuid(),
            Text = "THIS IS A SLIGHT LONG BUT NOT TOO LONG TEXT REPEATED TWICE THIS IS A SLIGHT LONG BUT NOT TOO LONG TEXT REPEATED TWICE",
            OrderNumber = 999999242,
            CorrelationId = "0ce1fb6b-0061-4d9d-825c-a4a3bd49b7b8-af9a9173-986b-44ea-887e-a52964b67eaa"
        };

        var correlationProperties = new[]
        {
            new TestCorrelationProperty(nameof(MySagaDizzle.Text), typeof(MySagaDizzle)),
            new TestCorrelationProperty(nameof(MySagaDizzle.OrderNumber), typeof(MySagaDizzle)),
            new TestCorrelationProperty(nameof(MySagaDizzle.CorrelationId), typeof(MySagaDizzle)),
        };

        await _storage.Insert(sagaData, correlationProperties);

        var stopwatch = Stopwatch.StartNew();

        for (var counter = 0; counter < iterations; counter++)
        {
            using var scope = new RebusTransactionScope();

            // fool MessageContext to thing there's an active incoming step context
            scope.TransactionContext.Items[StepContext.StepContextKey] = new object();

            if (MessageContext.Current == null)
            {
                throw new AssertionException("Expected MessageContext.Current to return something != NULL at this point");
            }

            var loadedSagaData = await _storage.Find(typeof(MySagaDizzle), "Id", sagaData.Id);
            await _storage.Update(loadedSagaData, correlationProperties);
        }

        var elapsedSeconds = stopwatch.Elapsed.TotalSeconds;

        Console.WriteLine($"Executed {iterations} updates in {elapsedSeconds:0.0} s - that's {iterations / elapsedSeconds:0.0} updates/s");
    }

    [Test]
    public async Task DoesNotThrowExceptionWhenInitializeOnOldSchema()
    {
        await CreatePreviousSchema();

        _storage.Initialize();

        _storage.EnsureTablesAreCreated();
    }

    [Test]
    public async Task CanRoundtripSagaOnOldSchema()
    {
        var noProps = Enumerable.Empty<ISagaCorrelationProperty>();

        await CreatePreviousSchema();

        _storage.Initialize();

        var sagaData = new MySagaDizzle { Id = Guid.NewGuid(), Text = "whee!" };

        await _storage.Insert(sagaData, noProps);

        var roundtrippedData = await _storage.Find(typeof(MySagaDizzle), "Id", sagaData.Id.ToString());

        Assert.That(roundtrippedData, Is.TypeOf<MySagaDizzle>());
        var sagaData2 = (MySagaDizzle)roundtrippedData;
        Assert.That(sagaData2.Text, Is.EqualTo(sagaData.Text));
    }

    class MySagaDizzle : ISagaData
    {
        public Guid Id { get; set; }
        public int Revision { get; set; }
        public string Text { get; set; }
        public int OrderNumber { get; set; }
        public string CorrelationId { get; set; }
    }

    async Task CreatePreviousSchema()
    {
        var createTableOldSchema =
            $@"

CREATE TABLE [dbo].[{_dataTableName}](
	[id] [uniqueidentifier] NOT NULL,
	[revision] [int] NOT NULL,
	[data] [nvarchar](max) NOT NULL,
	 CONSTRAINT [PK_{_dataTableName}] PRIMARY KEY CLUSTERED 
	(
		[id] ASC
	)
)

";

        var createTableOldSchema2 =
            $@"

CREATE TABLE [dbo].[{_indexTableName}](
	[saga_type] [nvarchar](40) NOT NULL,
	[key] [nvarchar](200) NOT NULL,
	[value] [nvarchar](200) NOT NULL,
	[saga_id] [uniqueidentifier] NOT NULL,
 CONSTRAINT [PK_{_indexTableName}] PRIMARY KEY CLUSTERED 
 (
	[key] ASC,
	[value] ASC,
	[saga_type] ASC
 ))
";

        Console.WriteLine($"Creating tables {_dataTableName} and {_indexTableName}");

        using var connection = await _connectionProvider.GetConnection();
        using (var command = connection.CreateCommand())
        {
            command.CommandText = createTableOldSchema;
            command.ExecuteNonQuery();
        }
        using (var command = connection.CreateCommand())
        {
            command.CommandText = createTableOldSchema2;
            command.ExecuteNonQuery();
        }

        await connection.Complete();
    }
}