using System;
using Hangfire.Mongo.Database;

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
            return Environment.GetEnvironmentVariable(DatabaseVariable) ?? DefaultDatabaseName;
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
                    Strategy = MongoMigrationStrategy.Drop,
                    BackupStrategy = MongoBackupStrategy.None
                }
            };
            return CreateStorage(storageOptions);
        }

        public static MongoStorage CreateStorage(MongoStorageOptions storageOptions)
        {
            return new MongoStorage(GetConnectionString(), GetDatabaseName(), storageOptions);
        }

        public static HangfireDbContext CreateConnection()
        {
            return CreateStorage().Connection;
        }
    }
#pragma warning restore 1591
}