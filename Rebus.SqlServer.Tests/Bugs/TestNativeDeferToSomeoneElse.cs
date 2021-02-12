using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using Rebus.Activation;
using Rebus.Config;
using Rebus.Exceptions;
using Rebus.Messages;
using Rebus.Pipeline;
using Rebus.Routing;
using Rebus.Routing.TypeBased;
using Rebus.Tests.Contracts;
using Rebus.Tests.Contracts.Extensions;

#pragma warning disable 1998

namespace Rebus.SqlServer.Tests.Bugs
{
    [TestFixture]
    public class TestNativeDeferToSomeoneElse : FixtureBase
    {
        static readonly string ConnectionString = SqlTestHelper.ConnectionString;

        [TestCase(true)]
        [TestCase(false)]
        public async Task ItWorks_SetDestinationHeaderFromTheOutside(bool usePipelineStep)
        {
            var receiver = new BuiltinHandlerActivator();

            Using(receiver);

            var receiverStarter = Configure.With(receiver)
                .Transport(t => t.UseSqlServer(new SqlServerTransportOptions(ConnectionString), "receiver"))
                .Create();

            var senderBus = Configure.With(new BuiltinHandlerActivator())
                .Transport(x => x.UseSqlServerAsOneWayClient(new SqlServerTransportOptions(ConnectionString)))
                .Routing(r => r.TypeBased().Map<string>("receiver"))
                .Options(o =>
                {
                    if (usePipelineStep)
                    {
                        o.Decorate<IPipeline>(c =>
                        {
                            var pipeline = c.Get<IPipeline>();
                            var step = new AutoDeferredRecipientStep(c.Get<IRouter>());

                            return new PipelineStepConcatenator(pipeline)
                                .OnSend(step, PipelineAbsolutePosition.Front);
                        });
                    }
                })
                .Start();

            Using(senderBus);

            var gotTheString = new ManualResetEvent(false);

            receiver.Handle<string>(async message => gotTheString.Set());
            receiverStarter.Start();

            var optionalHeaders = usePipelineStep
                ? new Dictionary<string, string>()
                : new Dictionary<string, string> { { Headers.DeferredRecipient, "receiver" } };

            await senderBus.Defer(TimeSpan.FromSeconds(1), "HEEELOOOOOO", optionalHeaders);

            gotTheString.WaitOrDie(TimeSpan.FromSeconds(5));
        }

        class AutoDeferredRecipientStep : IOutgoingStep
        {
            readonly IRouter _router;

            public AutoDeferredRecipientStep(IRouter router)
            {
                _router = router;
            }

            public async Task Process(OutgoingStepContext context, Func<Task> next)
            {
                var message = context.Load<Message>();
                if (message.Headers.TryGetValue(Headers.DeferredUntil, out _))
                {
                    if (!message.Headers.TryGetValue(Headers.DeferredRecipient, out var temp)
                        || temp == null)
                    {
                        try
                        {
                            message.Headers[Headers.DeferredRecipient] = await _router.GetDestinationAddress(message);
                        }
                        catch (Exception exception)
                        {
                            throw new RebusApplicationException(exception, "Could not automatically set recipient for deferred message");
                        }
                    }
                }

                await next();
            }
        }
    }
}
