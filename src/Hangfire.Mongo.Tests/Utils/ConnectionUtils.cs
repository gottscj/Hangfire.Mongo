using Hangfire.Mongo.Database;
using Hangfire.Mongo.Migration.Strategies;
using Hangfire.Mongo.Migration.Strategies.Backup;
using MongoDB.Driver;
using System;

[assembly: Xunit.TestFramework("Hangfire.Mongo.Tests.Utils.ConnectionUtils", "Hangfire.Mongo.Tests")]

namespace Hangfire.Mongo.Tests.Utils
{
#pragma warning disable 1591
    public class ConnectionUtils
    {
        private const string DefaultDatabaseName = @"Hangfire-Mongo-Tests";
        private static string ConnectionString = "mongodb://localhost:27017";
            // "mongodb://localhost:27017?replicaSet=rs0&readPreference=primary&ssl=false";

        public static MongoStorage CreateStorage(string databaseName = null)
        {
            var storageOptions = new MongoStorageOptions
            {
                MigrationOptions = new MongoMigrationOptions
                {
                    MigrationStrategy = new DropMongoMigrationStrategy(),
                    BackupStrategy = new NoneMongoBackupStrategy()
                }
            };
            return CreateStorage(storageOptions, databaseName);
        }

        
        public static MongoStorage CreateStorage(MongoStorageOptions storageOptions, string databaseName=null)
        {
            var mongoClientSettings = MongoClientSettings.FromConnectionString(ConnectionString);
            return new MongoStorage(mongoClientSettings, databaseName ?? DefaultDatabaseName, storageOptions);
        }

        public static HangfireDbContext CreateDbContext(string dbName = null)
        {
            return new HangfireDbContext(ConnectionString, dbName ?? DefaultDatabaseName);
        }

        public static void DropDatabase()
        {
            var client = new MongoClient(ConnectionString);
            client.DropDatabase(DefaultDatabaseName);
        }
    }
#pragma warning restore 1591
}
