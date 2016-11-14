using System;
using System.Threading.Tasks;
using NUnit.Framework;
using Rebus.Extensions;
using Rebus.Logging;
using Rebus.SqlServer.Subscriptions;
using Rebus.SqlServer.Tests.Subscriptions.This.Is.Just.An.Incredibly.Long.And.Silly.Namespace.Name.That.Needs.To.Be.Even.Longer.Because.It.Just.Needs.To.Be.Long.OK.But.Soon.It.Must.Be.Long.Enough.To.Exceed.That.Silly.Limit.In.SqlServer;
using Rebus.Tests.Contracts;

namespace Rebus.SqlServer.Tests.Subscriptions
{
    [TestFixture]
    public class TestSqlServerSubscriptionStorage : FixtureBase
    {
        SqlServerSubscriptionStorage _storage;

        protected override void SetUp()
        {
            SqlTestHelper.DropTable("Subscriptions");

            var loggerFactory = new ConsoleLoggerFactory(false);
            var connectionProvider = new DbConnectionProvider(SqlTestHelper.ConnectionString, loggerFactory);
            _storage = new SqlServerSubscriptionStorage(connectionProvider, "Subscriptions", true, loggerFactory);

            _storage.EnsureTableIsCreated();
        }

        [TestCase(typeof(SomeClass))]
        public async Task CanLookupSubscriberEventThoughTypeNameIsTooLongForSqlServerToHandle(Type type)
        {
            var topic = type.GetSimpleAssemblyQualifiedName();

            PrintTopic(topic);

            await _storage.RegisterSubscriber(topic, "bimse");

            var subscribers = await _storage.GetSubscriberAddresses(topic);

            Assert.That(subscribers, Is.EqualTo(new[] {"bimse"}));
        }

        [TestCase(typeof(SomeClass))]
        public async Task CanUnregisterSubscriberToo(Type type)
        {
            var topic = type.GetSimpleAssemblyQualifiedName();

            PrintTopic(topic);

            await _storage.RegisterSubscriber(topic, "bimse");
            await _storage.UnregisterSubscriber(topic, "bimse");

            var subscribers = await _storage.GetSubscriberAddresses(topic);

            Assert.That(subscribers.Length, Is.EqualTo(0));
        }

        static void PrintTopic(string topic)
        {
            Console.WriteLine($@"Here is the topic:

{topic}

(it's {topic.Length} characters)");
        }
    }

    namespace This.Is.Just.An.Incredibly.Long.And.Silly.Namespace.Name.That.Needs.To.Be.Even.Longer.Because.It.Just.Needs.To.Be.Long.OK.But.Soon.It.Must.Be.Long.Enough.To.Exceed.That.Silly.Limit.In.SqlServer
    {
        public class SomeClass
        {
        }
    }
}