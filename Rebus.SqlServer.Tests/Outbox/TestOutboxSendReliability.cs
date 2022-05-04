//using System;
//using System.Linq;
//using System.Threading.Tasks;
//using NUnit.Framework;
//using Rebus.Activation;
//using Rebus.Config;
//using Rebus.Config.Outbox;
//using Rebus.Tests.Contracts.Utilities;
//using Rebus.Transport;
//using Rebus.Transport.InMem;
//// ReSharper disable AccessToDisposedClosure
//#pragma warning disable CS4014
//#pragma warning disable CS1998

//namespace Rebus.SqlServer.Tests.Outbox;

//[TestFixture]
//public class TestOutboxSendReliability
//{
//    [TestCase(10)]
//    public async Task CanSendEvenThoughTransportIsUnreliable(int messageCount)
//    {
//        using var receivedMessagesCounter = new SharedCounter(initialValue: messageCount);
//        using var activator = new BuiltinHandlerActivator();

//        activator.Handle<string>(async _ => receivedMessagesCounter.Decrement());

//        var bus = Configure.With(activator)
//            .Transport(t =>
//            {
//                t.UseInMemoryTransport(new InMemNetwork(), "unreliableness");
//                t.ThrowSometimesWhenSendingMessages(successRate: 0.5);
//            })
//            .Outbox(o => o.StoreInSqlServer(SqlTestHelper.ConnectionString, "Outbox"))
//            .Start();

//        Task.Run(async () =>
//        {
//            try
//            {
//                var messages = Enumerable.Range(0, messageCount).Select(n => $"MESSAGE {n:00000}");

//                using var scope = new RebusTransactionScope();

//                foreach (var message in messages)
//                {
//                    await bus.SendLocal(message);
//                }

//                await scope.CompleteAsync();
//            }
//            catch (Exception exception)
//            {
//                Console.WriteLine($"AWW BUGGER SEND FAILED: {exception}");
//            }
//        });

//        receivedMessagesCounter.WaitForResetEvent(timeoutSeconds: 15);
//    }
//}
