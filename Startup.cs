using Coflnet.Sky.Core;
using Coflnet.Sky.Updater.Models;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.OpenApi.Models;
using Prometheus;

namespace Coflnet.Sky.Updater
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

            services.AddControllers();
            services.AddSwaggerGen(c =>
            {
                c.SwaggerDoc("v1", new OpenApiInfo { Title = "SkyUpdater", Version = "v1" });
            });
            services.AddJaeger(Configuration, 0.1);
            services.AddSingleton<ItemSkinHandler>();
            services.AddHostedService<ItemSkinHandler>(di => di.GetService<ItemSkinHandler>());
            services.AddHostedService<UpdaterManager>();
            services.AddSingleton<Kafka.KafkaCreator>();
            services.AddSingleton<ItemDetailsExtractor>();
            services.AddSingleton<ItemDetails>();
            services.AddSingleton<Topics>(di => Configuration.GetSection("TOPICS").Get<Topics>());
            services.AddSingleton<Items.Client.Api.IItemsApi, Items.Client.Api.ItemsApi>(sp => new Items.Client.Api.ItemsApi(Configuration["ITEMS_BASE_URL"]));
            services.AddResponseCaching();
            services.AddMemoryCache();
        }

        // This method gets called by the runtime. Use this method to configure the HTTP request pipeline.
        public void Configure(IApplicationBuilder app, IWebHostEnvironment env)
        {
            if (env.IsDevelopment())
            {
                app.UseDeveloperExceptionPage();
            }
            app.UseSwagger();
            app.UseSwaggerUI(c =>
            {
                c.SwaggerEndpoint("/swagger/v1/swagger.json", "SkyUpdater v1");
                c.RoutePrefix = "api";
            });

            app.UseRouting();
            app.UseResponseCaching();

            app.UseEndpoints(endpoints =>
            {
                endpoints.MapMetrics();
                endpoints.MapControllers();
            });
        }
    }
}
