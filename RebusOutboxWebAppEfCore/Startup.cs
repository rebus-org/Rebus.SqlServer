using System;
using System.Data;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Rebus.Config.Outbox;
using Rebus.Routing.TypeBased;
using Rebus.ServiceProvider;
using Rebus.SqlServer;
using Rebus.Transport;
using Rebus.Transport.InMem;
using RebusOutboxWebAppEfCore.Entities;
using RebusOutboxWebAppEfCore.Handlers;
using RebusOutboxWebAppEfCore.Messages;
using IDbConnection = Rebus.SqlServer.IDbConnection;

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
        public void ConfigureServices(IServiceCollection services)
        {
            services.AddControllersWithViews();

            static IDbConnection GetDbConnection(ITransactionContext transactionContext, IServiceProvider provider)
            {
                var scope = provider.CreateScope();
                transactionContext.OnDisposed(_ => scope.Dispose());
                var context = scope.ServiceProvider.GetRequiredService<WebAppDbContext>();
                var dbConnection = context.Database.GetDbConnection();
                if (dbConnection.State != ConnectionState.Open)
                {
                    context.Database.OpenConnection();
                }
                
                transactionContext.OnDisposed(_ => dbConnection.Dispose());

                var transaction = dbConnection.BeginTransaction();
                transactionContext.OnDisposed(_ => transaction.Dispose());
                transactionContext.OnCommitted(_ => transaction.CommitAsync());

                return new DbConnectionWrapper((SqlConnection)dbConnection, (SqlTransaction)transaction, managedExternally: false);
            }

            services.AddRebus(
                (configure, provider) => configure
                    .Transport(t => t.UseInMemoryTransport(new InMemNetwork(), "outbox-test"))
                    .Outbox(o => o.UseSqlServer(tc => GetDbConnection(tc, provider), "Outbox"))
                    .Routing(r => r.TypeBased().Map<ProcessMessageCommand>("outbox-test"))
            );

            services.AddRebusHandler<ProcessMessageCommandHandler>();

            services.AddDbContext<WebAppDbContext>(options => options.UseSqlServer("server=.; database=rebusoutboxwebapp; trusted_connection=true"));
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

            app.UseEndpoints(endpoints =>
            {
                endpoints.MapControllerRoute(
                    name: "default",
                    pattern: "{controller=Home}/{action=Index}/{id?}");
            });

            app.ApplicationServices.UseRebus();
        }
    }
}
