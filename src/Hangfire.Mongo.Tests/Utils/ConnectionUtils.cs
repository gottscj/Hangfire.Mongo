using System;
using System.Runtime.InteropServices;
using Hangfire.Mongo.Database;
using Hangfire.Mongo.Migration.Strategies;
using Hangfire.Mongo.Migration.Strategies.Backup;
using MongoDB.Driver;

namespace Hangfire.Mongo.Tests.Utils
{
#pragma warning disable 1591
    public static class ConnectionUtils
    {
        private const string DatabaseVariable = "Hangfire_Mongo_DatabaseName";
        private const string ConnectionStringTemplateVariable = "Hangfire_Mongo_ConnectionStringTemplate";

        private const string DefaultDatabaseName = @"Hangfire-Mongo-Tests";
        private const string DefaultConnectionStringTemplate = @"mongodb://localhost";

        public static string GetDatabaseName()
        {
            var framework = "Net46";
            if (RuntimeInformation.FrameworkDescription.Contains(".NET Core"))
            {
                framework = "NetCore";
            }
            else if (RuntimeInformation.FrameworkDescription.Contains("Mono"))
            {
                framework = "Mono";
            }
            return Environment.GetEnvironmentVariable(DatabaseVariable) ?? DefaultDatabaseName + "-" + framework;
        }

        public static string GetConnectionString()
        {
            return string.Format(GetConnectionStringTemplate(), GetDatabaseName());
        }

        private static string GetConnectionStringTemplate()
        {
            return Environment.GetEnvironmentVariable(ConnectionStringTemplateVariable) ?? DefaultConnectionStringTemplate;
        }

        public static MongoStorage CreateStorage()
        {
            var storageOptions = new MongoStorageOptions
            {
                MigrationOptions = new MongoMigrationOptions
                {
                    MigrationStrategy = new DropMongoMigrationStrategy(),
                    BackupStrategy = new NoneMongoBackupStrategy()
                }
            };
            return CreateStorage(storageOptions);
        }

        public static MongoStorage CreateStorage(MongoStorageOptions storageOptions)
        {
            var mongoClientSettings = MongoClientSettings.FromConnectionString(GetConnectionString());
            return new MongoStorage(mongoClientSettings, GetDatabaseName(), storageOptions);
        }

        public static HangfireDbContext CreateDbContext()
        {
            return new HangfireDbContext(GetConnectionString(), GetDatabaseName());
        }
    }
#pragma warning restore 1591
}