using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using Rebus.Activation;
using Rebus.Bus;
using Rebus.Config;
using Rebus.Logging;
using Rebus.Tests.Contracts;
using Rebus.Tests.Contracts.Extensions;
using Rebus.Tests.Contracts.Utilities;
// ReSharper disable ArgumentsStyleLiteral

namespace Rebus.SqlServer.Tests.Transport
{
    [TestFixture]
    public class TestSqlServerTransportCleanup : FixtureBase
    {
        BuiltinHandlerActivator _activator;
        ListLoggerFactory _loggerFactory;
        IBus _bus;

        protected override void SetUp()
        {
            var queueName = TestConfig.GetName("connection_timeout");

            _activator = new BuiltinHandlerActivator();

            Using(_activator);

            _loggerFactory = new ListLoggerFactory(outputToConsole: true);

            _bus = Configure.With(_activator)
                .Logging(l => l.Use(_loggerFactory))
                .Transport(t => t.UseSqlServer(new SqlServerTransportOptions(SqlTestHelper.ConnectionString), queueName))
                .Options(o => o.SetNumberOfWorkers(0))
                .Start();
        }

        [Test]
        public void DoesNotBarfInTheBackground()
        {
            var doneHandlingMessage = new ManualResetEvent(false);

            _activator.Handle<string>(async str =>
            {
                for (var count = 0; count < 5; count++)
                {
                    Console.WriteLine("waiting...");
                    await Task.Delay(TimeSpan.FromSeconds(20));
                }

                Console.WriteLine("done waiting!");

                doneHandlingMessage.Set();
            });

            _bus.Advanced.Workers.SetNumberOfWorkers(1);

            _activator.Bus.SendLocal("hej med dig min ven!").Wait();

            doneHandlingMessage.WaitOrDie(TimeSpan.FromMinutes(2));

            var logLinesAboveInformation = _loggerFactory
                .Where(l => l.Level >= LogLevel.Warn)
                .ToList();

            Assert.That(!logLinesAboveInformation.Any(), "Expected no warnings - got this: {0}", string.Join(Environment.NewLine, logLinesAboveInformation));
        }
    }
}
