using System;
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
        const string TableName = "SagaSnapshots";

        protected override void SetUp()
        {
            SqlTestHelper.DropTable(TableName);

            _activator = Using(new BuiltinHandlerActivator());

            Configure.With(_activator)
                .Transport(t => t.UseInMemoryTransport(new InMemNetwork(), "auditing-buggerino"))
                .Sagas(s => s.StoreInMemory())
                .Options(o =>
                {
                    o.EnableSagaAuditing().StoreInSqlServer(SqlTestHelper.ConnectionString, TableName);
                    o.SetMaxParallelism(1);
                })
                .Start();
        }

        [Test]
        public async Task ItWorks()
        {
            _activator.Register(() => new MySaga());

            await _activator.Bus.SendLocal("hej");
            await _activator.Bus.SendLocal("med");
            await _activator.Bus.SendLocal("dig");

            await Task.Delay(TimeSpan.FromSeconds(1));

            var snapshots = SqlTestHelper
                .Query<SagaSnapshot>(
                    $@"
                    SELECT 
                        [Id],
                        [Revision]
                    FROM
                        [{TableName}]"
                )
                .OrderBy(s => s.Revision)
                .ToList();

            Assert.That(snapshots.Count, Is.EqualTo(3));

            Assert.That(snapshots.Select(s => s.Revision), Is.EqualTo(new[] { 0, 1, 2 }), "Expected snapshots of revision 0, 1, and 2");
            Assert.That(snapshots.GroupBy(s => s.Id).Count(), Is.EqualTo(1), "Expected three snapshots for the same saga ID");
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