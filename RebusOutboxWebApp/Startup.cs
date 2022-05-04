using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Rebus.Config;
using Rebus.Config.Outbox;
using Rebus.Routing.TypeBased;
using Rebus.Transport.InMem;
using RebusOutboxWebApp.Extensions;
using RebusOutboxWebApp.Handlers;
using RebusOutboxWebApp.Messages;

namespace RebusOutboxWebApp
{
    public class Startup
    {
        const string OutboxConnectionString = "server=.; database=rebusoutboxwebapp; trusted_connection=true; encrypt=false";
        const string OutboxTableName = "Outbox";

        public Startup(IConfiguration configuration)
        {
            Configuration = configuration;
        }

        public IConfiguration Configuration { get; }

        // This method gets called by the runtime. Use this method to add services to the container.
        public void ConfigureServices(IServiceCollection services)
        {
            services.AddControllersWithViews();

            services.AddRebus(
                (configure, _) => configure
                    .Transport(t => t.UseInMemoryTransport(new InMemNetwork(), "outbox-test"))
                    .Outbox(o => o.StoreInSqlServer(OutboxConnectionString, OutboxTableName))
                    .Routing(r => r.TypeBased().Map<SendMessageCommand>("outbox-test"))
            );

            services.AddRebusHandler<SendMessageCommandHandler>();
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

            app.UseRebusOutbox(OutboxConnectionString);

            app.UseEndpoints(endpoints =>
            {
                endpoints.MapControllerRoute(
                    name: "default",
                    pattern: "{controller=Home}/{action=Index}/{id?}");
            });
        }
    }
}
