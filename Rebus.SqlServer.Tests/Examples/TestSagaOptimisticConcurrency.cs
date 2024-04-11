using System;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using NUnit.Framework;
using Rebus.Activation;
using Rebus.Config;
using Rebus.Logging;
using Rebus.Sagas;
using Rebus.Serialization;
using Rebus.Tests.Contracts;
using Rebus.Transport.InMem;

#pragma warning disable CS1998 // Async method lacks 'await' operators and will run synchronously

namespace Rebus.SqlServer.Tests.Examples;

[TestFixture]
[Description("Optimistic concurrency example that demonstrates how two messages will be handled concurrently, causing one of the saga updates to be rolled back and retried")]
public class TestSagaOptimisticConcurrency : FixtureBase
{
    protected override void SetUp()
    {
        base.SetUp();
        
        Using(new DisposableCallback(() =>
        {
            SqlTestHelper.DropTable("sagaindex");
            SqlTestHelper.DropTable("sagas");
        }));
    }

    [Test]
    public async Task ItWorks()
    {
        using var activator = new BuiltinHandlerActivator();

        activator.Register(() => new SampleSaga());

        var bus = Configure.With(activator)
            .Logging(l => l.Console(minLevel: LogLevel.Warn))
            .Transport(t => t.UseInMemoryTransport(new(), "sql-server-concurrency"))
            .Sagas(s => s.StoreInSqlServer(SqlTestHelper.ConnectionString, "sagas", "sagaindex"))
            .Start();

        await bus.SendLocal(new SampleCommand { Id = 77, WaitTimeout = TimeSpan.FromSeconds(1) });
        await bus.SendLocal(new SampleCommand { Id = 77, WaitTimeout = TimeSpan.FromSeconds(1) });

        // give enough time for both messages to be handled in parallel followed by one of them being re-handled because it got rolled back
        await Task.Delay(TimeSpan.FromSeconds(3));

        var sagaDataJson = SqlTestHelper.Query<string>("select cast(data as varchar(max)) from sagas where id = (select saga_id from sagaindex where [key]='MessageId' and value='77')").First();
        var sagaData = (SampleSagaData)new ObjectSerializer().Deserialize(Encoding.UTF8.GetBytes(sagaDataJson));

        Assert.That(sagaData.MessagesHandled, Is.EqualTo(2));
    }

    public class SampleSaga : Saga<SampleSagaData>, IAmInitiatedBy<StartCommand>, IAmInitiatedBy<SampleCommand>
    {
        protected override void CorrelateMessages(ICorrelationConfig<SampleSagaData> config)
        {
            config.Correlate<StartCommand>(m => m.Id, d => d.MessageId);
            config.Correlate<SampleCommand>(m => m.Id, d => d.MessageId);
        }

        public async Task Handle(StartCommand message)
        {
            Console.WriteLine($"Handle StartCommand '{message.Id}'");

            Data.MessageId = message.Id;
            Data.InProcess = false;

            Console.WriteLine($"End StartCommand '{message.Id}'");
        }

        public async Task Handle(SampleCommand message)
        {
            Console.WriteLine($"1. Handle SampleCommand '{message.Id}' WaitTimeout '{message.WaitTimeout}'. Data.MessageId = {Data.MessageId}; Data.InProcess = {Data.InProcess}");

            Data.MessagesHandled++;

            await Task.Delay(message.WaitTimeout);

            if (!Data.InProcess)
            {
                Console.WriteLine($"2. in if (!Data.InProcess). Data.InProcess = {Data.InProcess}");

                Data.InProcess = true;

                Console.WriteLine($"3. Data.InProcess = {Data.InProcess}");
            }

            Console.WriteLine($"4. End SampleCommand. Data.InProcess = {Data.InProcess}");
        }
    }

    public class SampleSagaData : ISagaData
    {
        public Guid Id { get; set; }
        public int MessageId { get; set; }
        public int Revision { get; set; }
        public bool InProcess { get; set; }
        public int MessagesHandled { get; set; }
    }

    public class StartCommand
    {
        public int Id { get; set; }
        public TimeSpan WaitTimeout { get; set; }
    }

    public class SampleCommand
    {
        public int Id { get; set; }
        public TimeSpan WaitTimeout { get; set; }
    }
}