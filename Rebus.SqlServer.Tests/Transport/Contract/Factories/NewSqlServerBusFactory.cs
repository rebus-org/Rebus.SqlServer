using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Rebus.Activation;
using Rebus.Bus;
using Rebus.Config;
using Rebus.Logging;
using Rebus.Tests.Contracts;
using Rebus.Tests.Contracts.Transports;
// ReSharper disable ArgumentsStyleNamedExpression

namespace Rebus.SqlServer.Tests.Transport.Contract.Factories
{
    public class NewSqlServerBusFactory : IBusFactory
    {
        readonly List<IDisposable> _stuffToDispose = new List<IDisposable>();

        public NewSqlServerBusFactory() => SqlTestHelper.DropAllTables();

        public IBus GetBus<TMessage>(string inputQueueAddress, Func<TMessage, Task> handler)
        {
            var builtinHandlerActivator = new BuiltinHandlerActivator();

            builtinHandlerActivator.Handle(handler);

            var tableName = "messages" + TestConfig.Suffix;

            SqlTestHelper.DropTable(tableName);

            var bus = Configure.With(builtinHandlerActivator)
                .Logging(l => l.Console(minLevel: LogLevel.Info))
                .Transport(t => t.UseSqlServerNew(SqlTestHelper.ConnectionString, inputQueueAddress))
                .Options(o =>
                {
                    o.SetNumberOfWorkers(10);
                    o.SetMaxParallelism(10);
                })
                .Start();

            _stuffToDispose.Add(bus);

            return bus;
        }

        public void Cleanup()
        {
            _stuffToDispose.ForEach(d => d.Dispose());
            _stuffToDispose.Clear();
        }
    }
}