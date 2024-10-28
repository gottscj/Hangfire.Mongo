using System;
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
            services.AddHangfire(async config =>
            {
                await using var mongoTestRunner = new MongoTestRunner();
                await mongoTestRunner.Start();

                // Read DefaultConnection string from appsettings.json
                var mongoUrlBuilder = new MongoUrlBuilder(mongoTestRunner.MongoConnectionString)
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
                    SlidingInvisibilityTimeout = TimeSpan.FromSeconds(5)
                };

                //config.UseLogProvider(new FileLogProvider());
                config.SetDataCompatibilityLevel(CompatibilityLevel.Version_180);
                config.UseMongoStorage(mongoClient, mongoUrlBuilder.DatabaseName, storageOptions)
                      .UseColouredConsoleLogProvider(LogLevel.Trace);
            });
            services.AddHangfireServer(options =>
            {
                options.Queues = new[] { "default", "not-default" };
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
