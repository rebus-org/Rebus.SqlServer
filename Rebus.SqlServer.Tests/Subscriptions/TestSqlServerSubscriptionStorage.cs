using System;
using System.Threading.Tasks;
using NUnit.Framework;
using Rebus.Extensions;
using Rebus.Logging;
using Rebus.SqlServer.Subscriptions;
using Rebus.SqlServer.Tests.Subscriptions.This.Is.Just.An.Incredibly.Long.And.Silly.Namespace.Name.That.Needs.To.Be.Even.Longer.Because.It.Just.Needs.To.Be.Long.OK.But.Soon.It.Must.Be.Long.Enough.To.Exceed.That.Silly.Limit.In.SqlServer;
using Rebus.Tests.Contracts;
#pragma warning disable 1998

namespace Rebus.SqlServer.Tests.Subscriptions
{
    [TestFixture]
    public class TestSqlServerSubscriptionStorage : FixtureBase
    {
        [Test]
        public async Task GetsAnAppropriateExceptionWhenAttemptingToRegisterSubscriberForTooLongTopic()
        {
            const string subscriberAddress = "subscriberino";

            var storage = GetStorage(false);

            var tooLongTopic = typeof(SomeClass).GetSimpleAssemblyQualifiedName();

            Console.WriteLine($@"The topic is pretty long:

{tooLongTopic}

");

            var aggregateException = Assert.Throws<AggregateException>(() =>
            {
                storage.RegisterSubscriber(tooLongTopic, subscriberAddress).Wait();
            });

            var baseException = aggregateException.GetBaseException();

            Console.WriteLine($@"Here is the exception:

{baseException}

");
        }

        [Test]
        [Description("Creates a subscribers table where the space for topic and subscriber address is distributed to reserve more space for the topic (which is probably best)")]
        public async Task WorksWithCustomSchema()
        {
            const string subscriberAddress = "subscriberino";

            var storage = GetStorage(true);

            var topic1 = typeof(SomeClass).GetSimpleAssemblyQualifiedName();
            var topic2 = typeof(AnotherClass).GetSimpleAssemblyQualifiedName();

            await storage.RegisterSubscriber(topic1, subscriberAddress);

            var subscribers1 = await storage.GetSubscriberAddresses(topic1);
            var subscribers2 = await storage.GetSubscriberAddresses(topic2);

            Assert.That(subscribers1, Is.EqualTo(new[] { subscriberAddress }));
            Assert.That(subscribers2, Is.EqualTo(new string[0]));
        }

        static SqlServerSubscriptionStorage GetStorage(bool createCustomSchema)
        {
            SqlTestHelper.DropTable("Subscriptions");

            var loggerFactory = new ConsoleLoggerFactory(false);
            var connectionProvider = new DbConnectionProvider(SqlTestHelper.ConnectionString, loggerFactory);
            var storage = new SqlServerSubscriptionStorage(connectionProvider, "Subscriptions", true, loggerFactory);

            if (createCustomSchema)
            {
                var tableName = TableName.Parse("Subscriptions");

                SqlTestHelper.Execute($@"

    CREATE TABLE {tableName.QualifiedName} (
	    [topic] [nvarchar](350) NOT NULL,
	    [address] [nvarchar](50) NOT NULL,
        CONSTRAINT [PK_{tableName.Schema}_{tableName.Name}] PRIMARY KEY CLUSTERED 
        (
	        [topic] ASC,
	        [address] ASC
        )
    )

");
            }
            else
            {
                storage.EnsureTableIsCreated();
            }

            storage.Initialize();

            return storage;
        }
    }

    namespace This.Is.Just.An.Incredibly.Long.And.Silly.Namespace.Name.That.Needs.To.Be.Even.Longer.Because.It.Just.Needs.To.Be.Long.OK.But.Soon.It.Must.Be.Long.Enough.To.Exceed.That.Silly.Limit.In.SqlServer
    {
        public class SomeClass
        {
        }

        public class AnotherClass
        {
        }
    }
}