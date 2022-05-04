using Microsoft.AspNetCore.Builder;
using Microsoft.Data.SqlClient;
using Rebus.Config.Outbox;
using Rebus.Transport;
// ReSharper disable UnusedParameter.Local

namespace RebusOutboxWebApp.Extensions;

static class RebusOutboxMiddleware
{
    public static void UseRebusOutbox(this IApplicationBuilder app, string connectionString)
    {
        app.Use(async (context, next) =>
        {
            // if you've set up DI resolution to fetch the current connection/transaction from a unit of work, you can do something like this:
            //var serviceProvider = context.RequestServices;
            //var connection = serviceProvider.GetRequiredService<SqlConnection>();
            //var transaction = serviceProvider.GetRequiredService<SqlTransaction>();

            // but in this case we'll work with a simulated unit of work here:
            await using var connection = new SqlConnection(connectionString);
            await connection.OpenAsync();

            await using var transaction = connection.BeginTransaction();

            using var scope = new RebusTransactionScope();
            
            scope.UseOutbox(connection, transaction);

            // process the web request
            await next();

            // insert outgoing messages
            await scope.CompleteAsync();

            // commit!
            await transaction.CommitAsync();
        });
    }
}