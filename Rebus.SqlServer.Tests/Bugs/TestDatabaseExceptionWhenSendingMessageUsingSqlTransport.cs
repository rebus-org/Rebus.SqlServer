using System;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using Rebus.Activation;
using Rebus.Bus;
using Rebus.Config;
using Rebus.Routing.TypeBased;
using Rebus.Tests.Contracts;
using Rebus.Tests.Contracts.Extensions;

// ReSharper disable AccessToDisposedClosure
#pragma warning disable CS1998

namespace Rebus.SqlServer.Tests.Bugs;

[TestFixture]
public class TestDatabaseExceptionWhenSendingMessageUsingSqlTransport : FixtureBase
{
    protected override void SetUp()
    {
        base.SetUp();

        SqlTestHelper.DropAllTables();
    }

    [Test]
    public async Task ItWorksAsExpected()
    {
        // create first bus that will send Message1 => queue2
        var bus1 = CreateBus(
            queueName: "queue1",
            routing: r => r.Map<Message1>(destinationAddress: "queue2")
        );

        // create second bus that will send Message2 => queue3 (which should because of queue3's insert trigger...)
        CreateBus(
            queueName: "queue2",
            handlers: a => a.Handle<Message1>(async (bus, _) => await bus.Send(new Message2())),
            routing: r => r.Map<Message2>(destinationAddress: "queue3")
        );

        // create third bus so the queue is there but without a handler (it should never get that far...)
        CreateBus(queueName: "queue3");

        // create trigger that throws an SQL exception on INSERT
        CreateTriggerThatThrowsExceptionWhenInsertingIntoTable("queue3");

        // create bus listening to the error queue, so we can verify that the message is dead-lettered as expected
        using var messageWasForwardedToErrorQueueAsExpected = new ManualResetEvent(initialState: false);

        CreateBus(
            queueName: "error",
            handlers: a => a.Handle<Message1>(async _ => messageWasForwardedToErrorQueueAsExpected.Set())
        );

        // go!
        await bus1.Send(new Message1());

        messageWasForwardedToErrorQueueAsExpected.WaitOrDie(TimeSpan.FromSeconds(5), 
            errorMessage: "Thought Message2 would have arrived in the error queue by now, but apparently it wasn't deadlettered as expected!");
    }

    void CreateTriggerThatThrowsExceptionWhenInsertingIntoTable(string queueName)
    {
        var triggerName = $"FailMessageTrigger_{queueName}";
        var sql = $@"

CREATE OR ALTER TRIGGER [dbo].[{triggerName}] ON [dbo].[{queueName}]
AFTER INSERT
AS 
BEGIN
    BEGIN
        THROW 51000, 'THIS IS THE INSERT TRIGGER FAILING ON PURPOSE', 1;  
    END
END
";

        SqlTestHelper.Execute(sql);

        Using(new DisposableCallback(() => SqlTestHelper.Execute($"DROP TRIGGER [dbo].[{triggerName}]")));
    }

    record Message1;
    record Message2;

    IBus CreateBus(
        string queueName,
        Action<BuiltinHandlerActivator> handlers = null,
        Action<TypeBasedRouterConfigurationExtensions.TypeBasedRouterConfigurationBuilder> routing = null
        )
    {
        Using(new DisposableCallback(() => SqlTestHelper.DropTable(queueName)));

        var activator = Using(new BuiltinHandlerActivator());

        handlers?.Invoke(activator);

        Configure.With(activator)
            .Transport(t => t.UseSqlServerInLeaseMode(new SqlServerLeaseTransportOptions(SqlTestHelper.ConnectionString), queueName))
            .Routing(r => routing?.Invoke(r.TypeBased()))
            .Start();

        return activator.Bus;
    }
}