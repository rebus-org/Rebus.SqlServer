# Rebus.SqlServer

[![install from nuget](https://img.shields.io/nuget/v/Rebus.SqlServer.svg?style=flat-square)](https://www.nuget.org/packages/Rebus.SqlServer)

Provides Microsoft SQL Server implementations for [Rebus](https://github.com/rebus-org/Rebus) for

* transport
* sagas
* subscriptions
* timeouts
* saga snapshots

![](https://raw.githubusercontent.com/rebus-org/Rebus/master/artwork/little_rebusbus2_copy-200x200.png)

---


# Which versions of SQL Server are supported?

Rebus' SQL package requires at least Microsoft SQL Server 2008.


# A word of caution regarding the SQL transport

Microsoft SQL Server is a relational database and not a queueing system. 

While it does provide the necessary mechanisms to implement queues, it's not optimized for the type of operations required to implement high-performance queues.

Therefore, please only use the SQL transport if your requirements are fairly modest (and what that means in practice probably depends a lot on the hardware available to you).


# Configuration examples

The Rebus configuration spell goes either 
```csharp
services.AddRebus(configure => configure.(...));
```
or
```csharp
Configure.With(...)
	.(...)
	.Start();
```
depending on whether you're using Microsoft DI or some other IoC container.

The following configuration examples will use the Microsoft DI-style of configuration, but the use of Rebus' configuration extensions is the same regardless of which type
of configuration you are using, so it should be fairly easy to convert to the style you need.


## Transport

Rebus only really requires one part of its configuration: A configuration of the "transport" (i.e. which queueing system, you're going to use). 

The SQL transport is not recommended for heavier workloads, but it can be fine in cases where you do not require a super-high throughput. Here's how to configure it
(in this case using the name `queue-name` as the name of the instance's input queue):

```csharp
services.AddRebus(
	configure => configure
		.Transport(t => t.UseSqlServer(connectionString, "queue-name"))
);
```


## Sagas

To configure Rebus to store sagas in SQL Server, you can do it like this (using the table 'Sagas' for the saga data, and 'SagaIndex' for the corresponding correlation properties):

```csharp
services.AddRebus(
	configure => configure
		.(...)
		.Sagas(t => t.StoreInSqlServer(connectionString, "Sagas", "SagaIndex"))
);
```


## Subscriptions

To configure Rebus to store subscriptions in SQL Server, you can do it like this (using the table 'Subscriptions'):

```csharp
services.AddRebus(
	configure => configure
		.(...)
		.Subscriptions(t => t.StoreInSqlServer(connectionString, "Subscriptions", isCentralized: true))
);
```

Please note the use of `isCentralized: true` – it indicates that the subscription storage is "centralized", meaning that both subscribers and publishers use the same storage.

If you use the `isCentralized: false` option, then subscribers need to know the queue names of the publishers of the events they want to subscribe to, and then they will subscribe by sending a message to the publisher.

Using `isCentralized: true` makes the most sense in most scenarios, as it's easier to work with.


## Timeouts

If you're using a transport that does not natively support "timeouts" (also known as "deferred messages", or "messages sent into the future" 🙂), you can configure one of your Rebus instances to function
as a "timeout manager". The timeout manager must have some kind of timeout storage configured, and you can use SQL Server to do that.

You configure it like this (here using RabbitMQ as the transport):

```csharp
services.AddRebus(
	configure => configure
		.Transport(t => t.UseRabbitMq(connectionString, "timeout_manager"))
		.Timeouts(t => t.StoreInSqlServer(connectionString, "Timeouts"))
);
```

In most cases, it can be super nice to simply configure one single timeout manager with a globally known queue name (e.g. "timeout_manager") and then make use of it from all other Rebus instances by
configuring them to use the timeout manager for deferred messages:

```csharp
services.AddRebus(
	configure => configure
		.Transport(t => t.UseRabbitMq(connectionString, "another-eueue"))
		.Timeouts(t => t.UseExternalTimeoutManager("timeout_manager"))
);
```

This will cause `someMessage` to be sent to the timeout manager when you `await bus.Defer(TimeSpan.FromMinutes(5), someMessage)`, which will store it in its timeouts database for 5 minutes before sending it to whoever was configured as the recipient of `someMessage`.



