using System;
using NUnit.Framework;
using Rebus.Activation;
using Rebus.Config;
using Rebus.Config.Outbox;
using Rebus.Injection;
using Rebus.Tests.Contracts;
using Rebus.Transport.InMem;

namespace Rebus.SqlServer.Tests.Bugs;

[TestFixture]
[Description("Tried (without success) to replicate missing inner exception when using a malformed connection string")]
public class TestErrorMessageWhenConnectionStringHasErrors : FixtureBase
{
    [Test]
    public void FehlermeldungIstGeil_Transport()
    {
        using var activator = new BuiltinHandlerActivator();

        var exception = Assert.Throws<ResolutionException>(() => Configure.With(activator)
            .Transport(t => t.UseSqlServer(new SqlServerTransportOptions("server=.; inital catalog=whatever"), "msgs"))
            .Start());

        Console.WriteLine(exception);

        Assert.That(exception.ToString(), Contains.Substring("inital catalog"));
    }

    [Test]
    public void FehlermeldungIstGeil_Subscriptions()
    {
        using var activator = new BuiltinHandlerActivator();

        var exception = Assert.Throws<ResolutionException>(() => Configure.With(activator)
            .Transport(t => t.UseInMemoryTransport(new InMemNetwork(), "doesn't matter"))
            .Subscriptions(s => s.StoreInSqlServer("server=.; inital catalog=whatever", "subs"))
            .Start());

        Console.WriteLine(exception);

        Assert.That(exception.ToString(), Contains.Substring("inital catalog"));
    }

    [Test]
    public void FehlermeldungIstGeil_Sagas()
    {
        using var activator = new BuiltinHandlerActivator();

        var exception = Assert.Throws<ResolutionException>(() => Configure.With(activator)
            .Transport(t => t.UseInMemoryTransport(new InMemNetwork(), "doesn't matter"))
            .Sagas(s => s.StoreInSqlServer("server=.; inital catalog=whatever", "sagas", "saga_index"))
            .Start());

        Console.WriteLine(exception);

        Assert.That(exception.ToString(), Contains.Substring("inital catalog"));
    }

    [Test]
    public void FehlermeldungIstGeil_Timeouts()
    {
        using var activator = new BuiltinHandlerActivator();

        var exception = Assert.Throws<ResolutionException>(() => Configure.With(activator)
            .Transport(t => t.UseInMemoryTransport(new InMemNetwork(), "doesn't matter"))
            .Timeouts(s => s.StoreInSqlServer("server=.; inital catalog=whatever", "timeouts"))
            .Start());

        Console.WriteLine(exception);

        Assert.That(exception.ToString(), Contains.Substring("inital catalog"));
    }

    [Test]
    public void FehlermeldungIstGeil_Outbox()
    {
        using var activator = new BuiltinHandlerActivator();

        var exception = Assert.Throws<ResolutionException>(() => Configure.With(activator)
            .Transport(t => t.UseInMemoryTransport(new InMemNetwork(), "doesn't matter"))
            .Outbox(s => s.StoreInSqlServer("server=.; inital catalog=whatever", "outbox"))
            .Start());

        Console.WriteLine(exception);

        Assert.That(exception.ToString(), Contains.Substring("inital catalog"));
    }
}