using System;
using System.IO;
using Hangfire.Mongo.Migration.Strategies;
using Hangfire.Mongo.Migration.Strategies.Backup;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using MongoDB.Driver;
using LogLevel = Hangfire.Logging.LogLevel;

namespace Hangfire.Mongo.Sample.ASPNetCore
{
    public class Startup
    {
        public Startup(IHostEnvironment env)
        {
            var builder = new ConfigurationBuilder()
                .SetBasePath(env.ContentRootPath)
                .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
                .AddJsonFile($"appsettings.{env.EnvironmentName}.json", optional: true)
                .AddEnvironmentVariables();
            Configuration = builder.Build();
        }

        public IConfigurationRoot Configuration { get; }

        // This method gets called by the runtime. Use this method to add services to the container.
        public void ConfigureServices(IServiceCollection services)
        {
            // Add framework services.
            services.AddHangfire(config =>
            {

                var runner = new MongoRunner().Start();
                services.AddSingleton(runner);
                
                // Read DefaultConnection string from appsettings.json
                var mongoUrlBuilder = new MongoUrlBuilder(runner.ConnectionString)
                {
                    DatabaseName = "hangfire"
                };
                var mongoClient = new MongoClient(mongoUrlBuilder.ToMongoUrl());
                
                var storageOptions = new MongoStorageOptions
                {
                    MigrationOptions = new MongoMigrationOptions
                    {
                        MigrationStrategy = new MigrateMongoMigrationStrategy(),
                        BackupStrategy = new CollectionMongoBackupStrategy()
                    },
                    UseNotificationsCollection = true
                };
                
                //config.UseLogProvider(new FileLogProvider());
                config.UseColouredConsoleLogProvider(LogLevel.Info);
                config.UseMongoStorage(mongoClient, mongoUrlBuilder.DatabaseName, storageOptions);
            });
            services.AddHangfireServer(options =>
            {
                options.Queues = new[] {"default", "notDefault"};
            });
            services.AddMvc(c => c.EnableEndpointRouting = false);
            
        }

        // This method gets called by the runtime. Use this method to configure the HTTP request pipeline.
        public void Configure(IApplicationBuilder app, IWebHostEnvironment env, ILoggerFactory loggerFactory)
        {
            app.UseHangfireDashboard();
            app.UseDeveloperExceptionPage();
            app.UseBrowserLink();

            app.UseStaticFiles();

            app.UseMvc(routes =>
            {
                routes.MapRoute(
                    name: "default",
                    template: "{controller=Home}/{action=Index}/{id?}");
            });
        }
    }
}
