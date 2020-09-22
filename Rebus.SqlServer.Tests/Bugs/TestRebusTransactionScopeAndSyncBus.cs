using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;
using NUnit.Framework;
using Rebus.Activation;
using Rebus.Config;
using Rebus.Extensions;
using Rebus.Messages;
using Rebus.SqlServer.Subscriptions;
using Rebus.SqlServer.Tests.Extensions;
using Rebus.SqlServer.Transport;
using Rebus.Tests.Contracts;
using Rebus.Tests.Contracts.Utilities;
using Rebus.Threading.TaskParallelLibrary;
using Rebus.Transport;
// ReSharper disable MethodHasAsyncOverload
// ReSharper disable UnusedVariable

namespace Rebus.SqlServer.Tests.Bugs
{
    [TestFixture]
    [Description("Just a quick test to verify that the syncbus API will enlist in RebusTransactionScope just like all other bus operations (copied from Rebus core to work with SQL transport)")]
    public class VerifySyncBusAndTransactionScope : FixtureBase
    {
        private SqlServerSubscriptionStorage _subscriptionStorage;
        private SqlServerTransport _subscriberTransport;

        protected override void SetUp()
        {
            // start clean
            SqlTestHelper.DropAllTables();

            // end clean
            Using(new DisposableCallback(SqlTestHelper.DropAllTables));

            var loggerFactory = new ListLoggerFactory();
            var connectionProvider = new DbConnectionProvider(SqlTestHelper.ConnectionString, loggerFactory);

            _subscriptionStorage = new SqlServerSubscriptionStorage(connectionProvider, "Subscriptions", isCentralized: true, loggerFactory);
            _subscriptionStorage.EnsureTableIsCreated();
            _subscriptionStorage.Initialize();

            _subscriberTransport = Using(new SqlServerTransport(connectionProvider, "subscriber", loggerFactory, new TplAsyncTaskFactory(loggerFactory), new FakeRebusTime(), new SqlServerTransportOptions(connectionProvider)));
            _subscriberTransport.EnsureTableIsCreated();
            _subscriberTransport.Initialize();
        }

        [Test]
        public async Task OnlyReceivesPublishedEventWhenRebusTransactionScopeIsCompleted()
        {
            // manually register the subscriber transport as a subscriber
            await _subscriptionStorage.RegisterSubscriber(typeof(TestEvent).GetSimpleAssemblyQualifiedName(), "subscriber");

            var bus = Configure.With(new BuiltinHandlerActivator())
                .Subscriptions(config => config.StoreInSqlServer(SqlTestHelper.ConnectionString, "Subscriptions"))
                .Transport(configurer => configurer.UseSqlServer(new SqlServerTransportOptions(SqlTestHelper.ConnectionString), "Test"))
                .Start();

            // this event should be published and received successfully by the subscriber
            using (var scope = new RebusTransactionScope())
            {
                bus.Advanced.SyncBus.Publish(new TestEvent("completed"));
                scope.Complete();
            }

            // this event is published, but the scope is not completed - therefore, it should NOT be received
            using (var scope = new RebusTransactionScope())
            {
                bus.Advanced.SyncBus.Publish(new TestEvent("not completed"));
                // this scope is intentionally not completed!
                //scope.Complete();
            }

            var messages = _subscriberTransport.GetMessages().ToList();
            Assert.That(messages.Count, Is.EqualTo(1), 
                "Expected one single event to have been received, because only one of the scopes were completed");

            var transportMessage = messages.First();
            Assert.That(transportMessage.Headers.GetValue(Headers.Type), Is.EqualTo(typeof(TestEvent).GetSimpleAssemblyQualifiedName()), 
                "Expected the type header to say 'TestEvent' (by the type's simple assembly-qualified name)");

            var json = Encoding.UTF8.GetString(transportMessage.Body);
            var testEvent = JsonConvert.DeserializeObject<TestEvent>(json);
            Assert.That(testEvent.Label, Is.EqualTo("completed"), 
                "Expected the received event to be the one with the label saying 'completed'");
        }

        class TestEvent
        {
            public string Label { get; }

            public TestEvent(string label) => Label = label;
        }
    }
}
