﻿using System;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;

using NUnit.Framework;

using Rebus.Activation;
using Rebus.Config;
using Rebus.Logging;
using Rebus.Sagas;
using Rebus.Tests.Contracts;
using Rebus.Tests.Contracts.Utilities;
using Rebus.Transport.InMem;

#pragma warning disable 1998

namespace Rebus.SqlServer.Tests.Sagas
{
    [TestFixture]
    public class TestSqlSagaStorageSpeed : FixtureBase
    {
        readonly string _dataTableName = TestConfig.GetName("sagas");
        readonly string _indexTableName = TestConfig.GetName("sagaindex");

        BuiltinHandlerActivator _activator;
        IBusStarter _starter;

        protected override void SetUp()
        {
            _activator = new BuiltinHandlerActivator();

            Using(_activator);

            SqlTestHelper.DropTable(_indexTableName);
            SqlTestHelper.DropTable(_dataTableName);

            _starter = Configure.With(_activator)
                .Logging(l => l.Console(LogLevel.Warn))
                .Transport(t => t.UseInMemoryTransport(new InMemNetwork(), "saga-perf"))
                .Sagas(s => s.StoreInSqlServer(SqlTestHelper.ConnectionString, _dataTableName, _indexTableName))
                .Options(o =>
                {
                    o.SetNumberOfWorkers(1);
                    o.SetMaxParallelism(1);
                })
                .Create();
        }

        protected override void TearDown()
        {
            SqlTestHelper.DropTable(_indexTableName);
            SqlTestHelper.DropTable(_dataTableName);
        }

        [Test]
        public async Task CheckTimes()
        {
            var counter = new SharedCounter(2);

            _activator.Register(() => new LongStringSaga(counter));
            _starter.Start();

            var longString = string.Join("/", Enumerable.Repeat("long string", 100000));

            Console.WriteLine($"Sending string with {longString.Length} characters...");

            await _activator.Bus.SendLocal($"secret-id/{longString}");

            Console.WriteLine("Sending short string...");

            var stopwatch = Stopwatch.StartNew();

            await _activator.Bus.SendLocal("secret-id/hej med dig min ven!");

            counter.WaitForResetEvent(timeoutSeconds: 10);

            var elapsed = stopwatch.Elapsed;

            Console.WriteLine($"Elapsed s: {elapsed.TotalSeconds:0.0}");
        }

        class LongStringSaga : Saga<LongStringSagaData>, IAmInitiatedBy<string>
        {
            readonly SharedCounter _counter;

            public LongStringSaga(SharedCounter counter)
            {
                _counter = counter;
            }

            protected override void CorrelateMessages(ICorrelationConfig<LongStringSagaData> config)
            {
                config.Correlate<string>(stringMessage => stringMessage.Split('/').First(), d => d.CorrelationId);
            }

            public async Task Handle(string stringMessage)
            {
                var lengthBefore = Data.LongString?.Length ?? 0;

                Data.LongString += stringMessage;

                var lengthAfter = Data.LongString.Length;

                Console.WriteLine($"Stored string length increased from {lengthBefore / 1000} k to {lengthAfter / 1000} k");

                _counter.Decrement();
            }
        }

        class LongStringSagaData : ISagaData
        {
            public Guid Id { get; set; }
            public int Revision { get; set; }
            public string CorrelationId { get; set; }
            public string LongString { get; set; }
        }
    }
}