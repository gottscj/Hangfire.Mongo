using Owin;

namespace Hangfire.Mongo.Sample
{
    public partial class Startup
    {
        public void ConfigureHangfire(IAppBuilder app)
        {
            var migrationOptions = new MongoStorageOptions
            {
                MigrationOptions = new MongoMigrationOptions
                {
                    Strategy = MongoMigrationStrategy.Migrate,
                }
            };

            GlobalConfiguration.Configuration.UseMongoStorage("mongodb://localhost", "hangfire-mongo-sample", migrationOptions);
            //GlobalConfiguration.Configuration.UseMongoStorage(new MongoClientSettings()
            //{
            //    // ...
            //    IPv6 = true
            //}, "hangfire-mongo-sample");

            app.UseHangfireServer();
            app.UseHangfireDashboard();
        }
    }
}
