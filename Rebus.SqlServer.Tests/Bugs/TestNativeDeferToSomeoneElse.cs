using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using Rebus.Activation;
using Rebus.Config;
using Rebus.Messages;
using Rebus.Routing.TypeBased;
using Rebus.Tests.Contracts;
using Rebus.Tests.Contracts.Extensions;
// ReSharper disable AccessToDisposedClosure

#pragma warning disable 1998

namespace Rebus.SqlServer.Tests.Bugs;

[TestFixture]
public class TestNativeDeferToSomeoneElse : FixtureBase
{
    static readonly string ConnectionString = SqlTestHelper.ConnectionString;

    [Test]
    public async Task CanDeferWithImplicitRouting_OneWayClientSender()
    {
        using var receiver = new BuiltinHandlerActivator();
        using var receiverGotTheString = new ManualResetEvent(false);

        receiver.Handle<string>(async _ => receiverGotTheString.Set());

        Configure.With(receiver)
            .Transport(t => t.UseSqlServer(new SqlServerTransportOptions(ConnectionString), "receiver"))
            .Start();

        using var sender = Configure.With(new BuiltinHandlerActivator())
             .Transport(x => x.UseSqlServerAsOneWayClient(new SqlServerTransportOptions(ConnectionString)))
             .Routing(r => r.TypeBased().Map<string>("receiver"))
             .Start();

        await sender.Defer(TimeSpan.FromSeconds(0.2), "HEEELOOOOOO");

        receiverGotTheString.WaitOrDie(TimeSpan.FromSeconds(5));
    }

    [Test]
    public async Task CanDeferWithImplicitRouting_NormalSender()
    {
        using var receiver = new BuiltinHandlerActivator();
        using var receiverGotTheString = new ManualResetEvent(false);

        receiver.Handle<string>(async _ => receiverGotTheString.Set());

        Configure.With(receiver)
            .Transport(t => t.UseSqlServer(new SqlServerTransportOptions(ConnectionString), "receiver"))
            .Start();

        using var sender = Configure.With(new BuiltinHandlerActivator())
             .Transport(x => x.UseSqlServer(new SqlServerTransportOptions(ConnectionString), "sender"))
             .Routing(r => r.TypeBased().Map<string>("receiver"))
             .Start();

        await sender.Defer(TimeSpan.FromSeconds(0.2), "HEEELOOOOOO");

        receiverGotTheString.WaitOrDie(TimeSpan.FromSeconds(5));
    }

    [Test]
    public async Task CanDeferWithExplicitRouting_AdvancedApi()
    {
        using var receiver = new BuiltinHandlerActivator();
        using var receiverGotTheString = new ManualResetEvent(false);

        receiver.Handle<string>(async _ => receiverGotTheString.Set());

        Configure.With(receiver)
            .Transport(t => t.UseSqlServer(new SqlServerTransportOptions(ConnectionString), "receiver"))
            .Start();

        using var sender = Configure.With(new BuiltinHandlerActivator())
             .Transport(x => x.UseSqlServerAsOneWayClient(new SqlServerTransportOptions(ConnectionString)))
             .Routing(r => r.TypeBased().Map<string>("doesn't exist"))
             .Start();

        await sender.Advanced.Routing.Defer("receiver", TimeSpan.FromSeconds(0.2), "HEEELOOOOOO");

        receiverGotTheString.WaitOrDie(TimeSpan.FromSeconds(5));
    }

    [Test]
    public async Task CanDeferWithExplicitRouting_UsingHeader()
    {
        using var receiver = new BuiltinHandlerActivator();
        using var receiverGotTheString = new ManualResetEvent(false);

        receiver.Handle<string>(async _ => receiverGotTheString.Set());

        Configure.With(receiver)
            .Transport(t => t.UseSqlServer(new SqlServerTransportOptions(ConnectionString), "receiver"))
            .Start();

        using var sender = Configure.With(new BuiltinHandlerActivator())
             .Transport(x => x.UseSqlServerAsOneWayClient(new SqlServerTransportOptions(ConnectionString)))
             .Routing(r => r.TypeBased().Map<string>("sender")) //< this one is not supposed to receive the message
             .Start();

        var headers = new Dictionary<string, string> { [Headers.DeferredRecipient] = "receiver" };

        await sender.Defer(TimeSpan.FromSeconds(0.2), "HEEELOOOOOO", headers);

        receiverGotTheString.WaitOrDie(TimeSpan.FromSeconds(5));
    }
}