using System;
using System.Data.Common;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Rebus.Config;
using Rebus.Config.Outbox;
using Rebus.Pipeline;
using Rebus.Retry.Simple;
using Rebus.Routing.TypeBased;
using Rebus.SqlServer.Outbox;
using Rebus.Transport;
using Rebus.Transport.InMem;
using RebusOutboxWebAppEfCore.Entities;
using RebusOutboxWebAppEfCore.Handlers;
using RebusOutboxWebAppEfCore.Messages;

namespace RebusOutboxWebAppEfCore
{
    public class Startup
    {
        public Startup(IConfiguration configuration)
        {
            Configuration = configuration;
        }

        public IConfiguration Configuration { get; }

        // This method gets called by the runtime. Use this method to add services to the container.
        string ConnectionString => Configuration.GetConnectionString("RebusDatabase")
                                   ?? throw new ApplicationException("Could not find 'RebusDatabase' connection string");

        /// <summary>
        /// Use this little object to carry connection+transaction around in a neat way
        /// </summary>
        record CurrentConnection(SqlConnection Connection, SqlTransaction Transaction);

        public void ConfigureServices(IServiceCollection services)
        {
            services.AddControllersWithViews();

            services.AddHttpContextAccessor();

            services.AddRebus(
                (configure, _) => configure
                    .Transport(t => t.UseInMemoryTransport(new InMemNetwork(), "outbox-test"))
                    .Outbox(o => o.StoreInSqlServer(ConnectionString, "Outbox"))
                    .Routing(r => r.TypeBased().Map<ProcessMessageCommand>("outbox-test"))
            );

            services.AddRebusHandler<ProcessMessageCommandHandler>();

            services.AddScoped(provider =>
            {
                CurrentConnection GetCurrentConnetionFromHttpContext()
                {
                    // if the WebAppDbContext is being resolved as part of the HTTP request pipeline, we get the connection like this
                    var httpContext = provider.GetRequiredService<IHttpContextAccessor>().HttpContext;
                    var items = httpContext?.Items;
                    if (items == null) return null;
                    var connection = (SqlConnection)items["current-db-connection"];
                    var transaction = (SqlTransaction)items["current-db-transaction"];
                    return new CurrentConnection(connection, transaction);
                }

                CurrentConnection GetCurrentConnectionFromRebusMessageContext()
                {
                    // if the WebAppDbContext is being resolved beacause Rebus is handling the message, we get the connection like this
                    var transactionContext = MessageContext.Current?.TransactionContext;
                    var outboxConnection = transactionContext?.Items["current-outbox-connection"] as OutboxConnection;
                    if (outboxConnection == null) return null;
                    return new CurrentConnection(outboxConnection.Connection, outboxConnection.Transaction);
                }

                CurrentConnection GetCurrentConnection() => GetCurrentConnetionFromHttpContext()
                                                            ?? GetCurrentConnectionFromRebusMessageContext()
                                                            ?? throw new InvalidOperationException("Can only get DB connection inside HTTP context or Rebus transaction context");

                var currentConnection = GetCurrentConnection();

                var builder = new DbContextOptionsBuilder<WebAppDbContext>();
                builder.UseSqlServer(currentConnection.Connection, options => options.EnableRetryOnFailure());

                var context = new WebAppDbContext(builder.Options);
                context.Database.UseTransaction(currentConnection.Transaction);
                
                return context;
            });
        }

        // This method gets called by the runtime. Use this method to configure the HTTP request pipeline.
        public void Configure(IApplicationBuilder app, IWebHostEnvironment env)
        {
            if (env.IsDevelopment())
            {
                app.UseDeveloperExceptionPage();
            }
            else
            {
                app.UseExceptionHandler("/Home/Error");
                // The default HSTS value is 30 days. You may want to change this for production scenarios, see https://aka.ms/aspnetcore-hsts.
                app.UseHsts();
            }
            app.UseHttpsRedirection();
            app.UseStaticFiles();

            app.UseRouting();

            app.UseAuthorization();

            // BEFORE app.UseEndpoints we register this little middleware that manages the database
            // connection+transaction when in an HTTP context
            app.Use(async (context, next) =>
            {
                // initialize our SQL connection + start transaction
                await using var sqlConnection = new SqlConnection(ConnectionString);
                await sqlConnection.OpenAsync();
                await using var sqlTransaction = sqlConnection.BeginTransaction();

                // stash in context so we can get to them elsewhere
                context.Items["current-db-connection"] = sqlConnection;
                context.Items["current-db-transaction"] = sqlTransaction;

                // start ambient scope to enlist all Rebus actions
                using var scope = new RebusTransactionScope();

                // enable the outbox
                scope.UseOutbox(sqlConnection, sqlTransaction);

                // call the rest of the pipeline
                await next();

                // commit the scope = Rebus saves outgoing messages to the database
                await scope.CompleteAsync();

                // commit the transaction
                await sqlTransaction.CommitAsync();
            });

            app.UseEndpoints(endpoints =>
            {
                endpoints.MapControllerRoute(
                    name: "default",
                    pattern: "{controller=Home}/{action=Index}/{id?}");
            });
        }
    }
}
