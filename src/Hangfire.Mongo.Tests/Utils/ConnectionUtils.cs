using System;
using System.Runtime.InteropServices;
using Hangfire.Mongo.Database;
using Hangfire.Mongo.Migration.Strategies;
using Hangfire.Mongo.Migration.Strategies.Backup;
using Mongo2Go;
using MongoDB.Driver;

namespace Hangfire.Mongo.Tests.Utils
{
#pragma warning disable 1591
    public static class ConnectionUtils
    {
        private const string DatabaseVariable = "Hangfire_Mongo_DatabaseName";

        private const string DefaultDatabaseName = @"Hangfire-Mongo-Tests";

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
            var mongoClientSettings = MongoClientSettings.FromConnectionString(GetRunner().ConnectionString);
            return new MongoStorage(mongoClientSettings, GetDatabaseName(), storageOptions);
        }

        public static HangfireDbContext CreateDbContext()
        {
            return new HangfireDbContext(GetRunner().ConnectionString, GetDatabaseName());
        }

        public static string GetConnectionString()
        {
            return GetRunner().ConnectionString;
        }
        private static readonly object SyncRoot = new object();
        private static MongoDbRunner _runner;
        public static void StopMongoDb()
        {
            lock (SyncRoot)
            {
                _runner?.Dispose();
            }
        }

        private static MongoDbRunner GetRunner()
        {
            lock (SyncRoot)
            { 
                _runner = _runner ?? MongoDbRunner.Start(singleNodeReplSet: false);
                return _runner;
            }
        }
    }
#pragma warning restore 1591
}