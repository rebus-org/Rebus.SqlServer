using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using NUnit.Framework;
using Rebus.Activation;
using Rebus.Auditing.Sagas;
using Rebus.Config;
using Rebus.Persistence.InMem;
using Rebus.Sagas;
using Rebus.Tests.Contracts;
using Rebus.Transport.InMem;
#pragma warning disable 1998

namespace Rebus.SqlServer.Tests.Bugs
{
    [TestFixture]
    public class TestBugWhenFinishingSagaAndAuditingIsEnabled : FixtureBase
    {
        BuiltinHandlerActivator _activator;
        IBusStarter _starter;
        const string TableName = "SagaSnapshots";

        protected override void SetUp()
        {
            SqlTestHelper.DropAllTables();

            _activator = Using(new BuiltinHandlerActivator());

            _starter = Configure.With(_activator)
                .Transport(t => t.UseInMemoryTransport(new InMemNetwork(), "auditing-buggerino"))
                .Sagas(s => s.StoreInMemory())
                .Options(o =>
                {
                    o.EnableSagaAuditing().StoreInSqlServer(SqlTestHelper.ConnectionString, TableName);
                    o.SetMaxParallelism(1);
                })
                .Create();

            Console.WriteLine($@"The test is now running - we have the following tables:

{string.Join(Environment.NewLine, SqlTestHelper.GetTableNames())}");
        }

        protected override void TearDown()
        {
            base.TearDown();

            Console.WriteLine($@"The test has finished running - we have the following tables:

{string.Join(Environment.NewLine, SqlTestHelper.GetTableNames())}");
        }

        [Test]
        public async Task ItWorks()
        {
            var snapshotsBefore = QuerySagaSnaps()
                .OrderBy(s => s.Revision)
                .ToList();

            Console.WriteLine($@"Found these snapshots BEFORE the test:

{string.Join(Environment.NewLine, snapshotsBefore.Select(s => $"{s.Id} / {s.Revision}"))}

!!!!!");

            _activator.Register(() => new MySaga());

            _starter.Start();

            await _activator.Bus.SendLocal("hej");
            await _activator.Bus.SendLocal("med");
            await _activator.Bus.SendLocal("dig");

            await Task.Delay(TimeSpan.FromSeconds(2));

            var snapshotsAfter = QuerySagaSnaps()
                .OrderBy(s => s.Revision)
                .ToList();

            Console.WriteLine($@"Found these snapshots AFTER the test:

{string.Join(Environment.NewLine, snapshotsAfter.Select(s => $"{s.Id} / {s.Revision}"))}

!!!!!");

            Assert.That(snapshotsAfter.Count, Is.EqualTo(3), $@"Only expected three snapshots - got these ids/revisions:

{string.Join(Environment.NewLine + Environment.NewLine, snapshotsAfter.Select(s => $"{s.Id} / {s.Revision}"))}");

            Assert.That(snapshotsAfter.Select(s => s.Revision), Is.EqualTo(new[] { 0, 1, 2 }), "Expected snapshots of revision 0, 1, and 2");
            Assert.That(snapshotsAfter.GroupBy(s => s.Id).Count(), Is.EqualTo(1), "Expected three snapshots for the same saga ID");
        }

        private static IEnumerable<SagaSnapshot> QuerySagaSnaps()
        {
            return SqlTestHelper
                .Query<SagaSnapshot>(
                    $@"
                    SELECT 
                        [Id],
                        [Revision]
                    FROM
                        [{TableName}]"
                );
        }

        class SagaSnapshot
        {
            public Guid Id { get; set; }
            public int Revision { get; set; }
        }

        class MySaga : Saga<MySagaData>, IAmInitiatedBy<string>
        {
            protected override void CorrelateMessages(ICorrelationConfig<MySagaData> config)
            {
                config.Correlate<string>(m => "static ID", s => s.CorrelationId);
            }

            public async Task Handle(string message)
            {
                Data.MessageCount++;

                if (Data.MessageCount < 3) return;

                MarkAsComplete();
            }
        }

        class MySagaData : SagaData
        {
            public string CorrelationId { get; set; }
            public int MessageCount { get; set; }
        }
    }
}
