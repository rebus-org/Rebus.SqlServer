using System.Collections.Generic;
using System;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using Rebus.Activation;
using Rebus.Bus;
using Rebus.Config;
using Rebus.Handlers;
using Rebus.Logging;
using Rebus.Sagas;
using Rebus.Tests.Contracts;
using Rebus.Tests.Contracts.Extensions;

// ReSharper disable AccessToDisposedClosure
#pragma warning disable CS1998 // Async method lacks 'await' operators and will run synchronously

namespace Rebus.SqlServer.Tests.Bugs;

[TestFixture]
[Description("Reproduce issue that would try to deserialize saga data correlated by ID as the wrong type")]
public class TestSagaDataMultipleStuff : FixtureBase
{
    [Test]
    public async Task ItWorks()
    {
        var sqlConnectionString = SqlTestHelper.ConnectionString;

        using var activator = new BuiltinHandlerActivator();

        var starter = Configure.With(activator)
            .Logging(l => l.Console(minLevel: LogLevel.Warn))
            .Transport(cfg => cfg.UseSqlServer(new SqlServerTransportOptions(sqlConnectionString), "test"))
            .Sagas(cfg => cfg.StoreInSqlServer(sqlConnectionString, "sagaData", "sagaIndex"))
            .Options(opt =>
            {
                opt.SetNumberOfWorkers(1);
                opt.SetMaxParallelism(1);
            })
            .Create();

        using var saga1Completed = new ManualResetEvent(initialState: false);
        using var saga2Completed = new ManualResetEvent(initialState: false);

        activator.Register((bus, _) => new Saga1(bus, saga1Completed));
        activator.Register((bus, _) => new Saga2(bus, saga2Completed));

        var bus = starter.Start();

        await bus.SendLocal(new StartSaga1 { Id = 1 });
        await bus.SendLocal(new StartSaga2 { Id = 2 });

        saga1Completed.WaitOrDie(TimeSpan.FromSeconds(5));
        saga2Completed.WaitOrDie(TimeSpan.FromSeconds(5));
    }

    public class StartSaga1
    {
        public int Id { get; set; }
    }

    public class StartSaga2
    {
        public int Id { get; set; }
    }

    public class Timeout
    {
        public int Id { get; set; }
    }

    public class Saga1 : Saga<Saga1.Saga1SagaData>, IAmInitiatedBy<StartSaga1>, IHandleMessages<Timeout>
    {
        readonly ManualResetEvent _completed;
        readonly IBus _bus;

        public Saga1(IBus bus, ManualResetEvent completed)
        {
            _bus = bus;
            _completed = completed;
        }

        protected override void CorrelateMessages(ICorrelationConfig<Saga1SagaData> config)
        {
            config.Correlate<StartSaga1>(x => x.Id, x => x.CustomId);
            config.CorrelateHeader<Timeout>("saga-id", x => x.Id);
        }

        public class Saga1SagaData : SagaData
        {
            public int CustomId { get; set; }
        }

        public async Task Handle(StartSaga1 message)
        {
            Console.WriteLine("Saga1");

            var optionalHeaders = new Dictionary<string, string> { { "saga-id", Data.Id.ToString() } };
            var timeout = new Timeout { Id = Data.CustomId };

            await _bus.DeferLocal(TimeSpan.FromSeconds(1), timeout, optionalHeaders);
        }

        public async Task Handle(Timeout message)
        {
            Console.WriteLine($"Timeout Saga1 {message.Id}");

            _completed.Set();
        }
    }

    public class Saga2 : Saga<Saga2.Saga2SagaData>, IAmInitiatedBy<StartSaga2>, IHandleMessages<Timeout>
    {
        readonly ManualResetEvent _completed;
        readonly IBus _bus;

        public Saga2(IBus bus, ManualResetEvent completed)
        {
            _bus = bus;
            _completed = completed;
        }

        protected override void CorrelateMessages(ICorrelationConfig<Saga2SagaData> config)
        {
            config.Correlate<StartSaga2>(x => x.Id, x => x.CustomId);
            config.CorrelateHeader<Timeout>("saga-id", x => x.Id);
        }

        public class Saga2SagaData : SagaData
        {
            public int CustomId { get; set; }
        }

        public async Task Handle(StartSaga2 message)
        {
            Console.WriteLine("Saga2");

            var timeout = new Timeout { Id = Data.CustomId };
            var optionalHeaders = new Dictionary<string, string> { { "saga-id", Data.Id.ToString() } };

            await _bus.DeferLocal(TimeSpan.FromSeconds(1), timeout, optionalHeaders);
        }

        public async Task Handle(Timeout message)
        {
            Console.WriteLine($"Timeout Saga2 {message.Id}");

            _completed.Set();
        }
    }
}