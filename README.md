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

# Transactional Outbox

The transactional outbox in Rebus.SqlServer ensures consistent and reliable message delivery by storing outgoing messages in an outbox table within the same SQL transaction as your other database operations. This approach helps prevent data inconsistencies in case of failures, as it ties the message dispatch to the success of your data changes.

## How to Configure the Transactional Outbox

### Basic Setup

To configure the transactional outbox with Rebus, use the `Outbox` extension method during the setup. Rebus allows you to use any transport, and the outbox will work in conjunction with your chosen transport.

```csharp
services.AddRebus(
    configure => configure
        .Transport(t => /* configure your transport here */)
        .Outbox(o => o.StoreInSqlServer(connectionString, "Outbox"))
);
```

- **`connectionString`**: The connection string to your SQL Server database.
- **`"Outbox"`**: The name of the table where outbox messages will be stored.

### Scenarios for Using the Transactional Outbox

#### Scenario 1: Outside a Rebus Handler

When you are outside a Rebus handler (e.g., in a web request or any other application context), you need to manage the SQL connection and transaction manually. Here's how you can do it:

```csharp
using var connection = new SqlConnection(connectionString);
await connection.OpenAsync();
using var transaction = connection.BeginTransaction();

try
{
    using var scope = new RebusTransactionScope();
    scope.UseOutbox(connection, transaction);

    // Perform your database operations using 'connection' and 'transaction'

    // Send messages using Rebus
    await bus.Send(new YourMessage());

    // Complete the Rebus transaction scope
    await scope.CompleteAsync();

    // Commit your transaction
    await transaction.CommitAsync();
}
catch (Exception ex)
{
    // Handle exceptions
    await transaction.RollbackAsync();
    // Log or rethrow the exception as needed
}
```

- **Explanation**:
  - You create and open a `SqlConnection` and begin a `SqlTransaction`.
  - Use `scope.UseOutbox(connection, transaction)` to inform Rebus to use your connection and transaction.
  - Perform your business logic and database operations within the transaction.
  - Send messages using Rebus; the messages will be stored in the outbox table within the same transaction.
  - After calling `scope.CompleteAsync()`, commit the transaction to ensure both your data changes and messages are persisted atomically.

#### Scenario 2: Inside a Rebus Handler

When inside a Rebus handler, Rebus manages the SQL connection and transaction for you. To include your database operations in the same transaction as Rebus, you can access the connection and transaction from the message context.

```csharp
public class YourMessageHandler : IHandleMessages<YourMessage>
{
    public async Task Handle(YourMessage message)
    {
        var messageContext = MessageContext.Current 
            ?? throw new InvalidOperationException("No message context available.");

        var transactionContext = messageContext.TransactionContext;
        var outboxConnection = (OutboxConnection)transactionContext.Items["current-outbox-connection"];

        var connection = outboxConnection.SqlConnection;
        var transaction = outboxConnection.SqlTransaction;

        // Perform your database operations using 'connection' and 'transaction'

        // Send messages using Rebus; they will be included in the same transaction
        await messageContext.Bus.Send(new AnotherMessage());
    }
}
```

- **Explanation**:
  - Retrieve the current `MessageContext`.
  - Access the `OutboxConnection` from the transaction context.
  - Use `outboxConnection.SqlConnection` and `outboxConnection.SqlTransaction` to perform your database operations.
  - Any messages you send will be stored in the outbox table within the same transaction.

## What Happens After the Message is Stored

Once a message is stored in the outbox table and the transaction is committed, Rebus handles the retrieval and forwarding of the message to its intended destination. This ensures that message dispatch is reliable and decoupled from your application logic.

- **High-Level Overview**:
  - The message remains in the outbox table until it is successfully dispatched.
  - Rebus periodically scans the outbox table for pending messages.
  - Upon successful delivery, messages are marked appropriately to prevent re-sending.
  - This mechanism ensures **at-least-once** delivery; your application should be designed to handle potential duplicate messages.

## Example Projects

For practical examples of how to implement the transactional outbox with Rebus.SqlServer, you can refer to the following sample projects in the GitHub repository:

- [RebusOutboxWebApp](https://github.com/rebus-org/Rebus.SqlServer/tree/master/RebusOutboxWebApp)
- [RebusOutboxWebAppEfCore](https://github.com/rebus-org/Rebus.SqlServer/tree/master/RebusOutboxWebAppEfCore)

These examples demonstrate the outbox implementation in different contexts, including integration with Entity Framework Core.

