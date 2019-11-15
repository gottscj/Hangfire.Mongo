using System.Configuration;
using Owin;

namespace Hangfire.Mongo.Sample
{
    public partial class Startup
    {
        public void ConfigureHangfire(IAppBuilder app)
        {
            // Read DefaultConnection string from Web.config
            var connectionString = ConfigurationManager.ConnectionStrings["DefaultConnection"].ConnectionString;

            var migrationOptions = new MongoStorageOptions
            {
                MigrationOptions = new MongoMigrationOptions
                {
                    Strategy = MongoMigrationStrategy.Migrate,
                    BackupStrategy = MongoBackupStrategy.Collections
                }
            };
            GlobalConfiguration.Configuration.UseMongoStorage(connectionString, migrationOptions);
            //GlobalConfiguration.Configuration.UseMongoStorage(new MongoClientSettings
            //{
            //    // ...
            //    IPv6 = true
            //}, "hangfire-mongo-sample");

            app.UseHangfireServer();
            app.UseHangfireDashboard();
        }
    }
}
