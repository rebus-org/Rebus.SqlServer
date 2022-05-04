//using System;
//using System.Data;
//using System.Threading;
//using System.Threading.Tasks;
//using Microsoft.Data.SqlClient;
//using NUnit.Framework;
//using Rebus.Activation;
//using Rebus.Config;
//using Rebus.Config.Outbox;
//using Rebus.Persistence.InMem;
//using Rebus.Pipeline;
//using Rebus.SqlServer.Outbox;
//using Rebus.Tests.Contracts;
//using Rebus.Tests.Contracts.Extensions;
//using Rebus.Transport;
//using Rebus.Transport.InMem;
//// ReSharper disable AccessToDisposedClosure
//#pragma warning disable CS1998

//namespace Rebus.SqlServer.Tests.Outbox;

//[TestFixture]
//public class TestOutboxUserTransaction : FixtureBase
//{
//    private const string CurrentDbConnectionKey = "current-db-connection";

//    protected override void SetUp()
//    {
//        base.SetUp();

//        SqlTestHelper.DropAllTables();

//        AsyncHelpers.RunSync(async () =>
//        {
//            using var connection = await GetDbConnection();

//            await using var command = connection.CreateCommand();

//            command.CommandText = @"
//CREATE TABLE [ReceivedTexts] (
//    [Id] int identity(1,1),
//    [Text] nvarchar(max) not null,
//    primary key ([Id])
//)
//";
//            await command.ExecuteNonQueryAsync();

//            await connection.Complete();
//        });
//    }

//    [Test]
//    public async Task ItWorks()
//    {
//        using var eventWasReceived = new ManualResetEvent(initialState: false);
//        using var activator = new BuiltinHandlerActivator();

//        activator.Handle<SaveText>(async (bus, context, message) =>
//        {
//            var connection = GetDbConnectionFromContext(context);
//            var text = message.Text;

//            await using var command = connection.CreateCommand();
//            command.CommandText = "INSERT INTO [ReceivedTexts] ([Text]) VALUES (@text)";
//            command.Parameters.Add("text", SqlDbType.NVarChar, text.Length.RoundUpToNextPowerOfTwo()).Value = text;
//            await command.ExecuteNonQueryAsync();

//            await bus.Publish(new TextSaved(text));
//        });

//        activator.Handle<TextSaved>(async _ => eventWasReceived.Set());

//        var bus = Configure.With(activator)
//            .Transport(t => t.UseInMemoryTransport(new InMemNetwork(), "test-queue"))
//            .Outbox(o => o.StoreInSqlServer(GetDbConnectionFromTransactionContext, "Outbox"))
//            .Subscriptions(s => s.StoreInMemory(new InMemorySubscriberStore()))
//            .Start();

//        await bus.Subscribe<TextSaved>();

//        await bus.SendLocal(new SaveText("HEJ MED DIG MIN VEN!"));

//        eventWasReceived.WaitOrDie(timeout: TimeSpan.FromSeconds(30), errorMessage: "The event was not received as expected");
//    }

//    static IDbConnection GetDbConnectionFromContext(IMessageContext context) => GetDbConnectionFromTransactionContext(context.TransactionContext);

//    static IDbConnection GetDbConnectionFromTransactionContext(ITransactionContext transactionContext) =>
//        (IDbConnection)transactionContext.Items.GetOrAdd(CurrentDbConnectionKey, _ =>
//        {
//            var connection = AsyncHelpers.GetSync(GetDbConnection);
//            transactionContext.OnCompleted(_ => connection.Complete());
//            transactionContext.OnDisposed(_ => connection.Dispose());
//            return connection;
//        });

//    record SaveText(string Text);

//    record TextSaved(string Text);

//    static async Task<DbConnectionWrapper> GetDbConnection()
//    {
//        var sqlConnection = new SqlConnection(SqlTestHelper.ConnectionString);
//        await sqlConnection.OpenAsync();
//        var transaction = sqlConnection.BeginTransaction();
//        var connection = new DbConnectionWrapper(sqlConnection, transaction, managedExternally: false);
//        return connection;
//    }
//}
