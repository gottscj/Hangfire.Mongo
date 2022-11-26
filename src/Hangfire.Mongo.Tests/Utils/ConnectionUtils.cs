using System;
using System.IO;
using System.Runtime.InteropServices;
using Hangfire.Mongo.Database;
using Hangfire.Mongo.Dto;
using Hangfire.Mongo.Migration.Strategies;
using Hangfire.Mongo.Migration.Strategies.Backup;
using Microsoft.Extensions.Logging.Abstractions;
using Mongo2Go;
using MongoDB.Bson.Serialization.Conventions;
using MongoDB.Driver;
using Xunit.Abstractions;
using Xunit.Sdk;

[assembly: Xunit.TestFramework("Hangfire.Mongo.Tests.Utils.ConnectionUtils", "Hangfire.Mongo.Tests")]

namespace Hangfire.Mongo.Tests.Utils
{
#pragma warning disable 1591
    public class ConnectionUtils : XunitTestFramework
    {
        private static Mongo2Go.MongoDbRunner _runner;
        private const string DefaultDatabaseName = @"Hangfire-Mongo-Tests";

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
            var mongoClientSettings = MongoClientSettings.FromConnectionString(_runner.ConnectionString);
            return new MongoStorage(mongoClientSettings, databaseName ?? DefaultDatabaseName, storageOptions);
        }

        public static HangfireDbContext CreateDbContext(string dbName = null)
        {
            ConventionRegistry.Register(
               "Hangfire Mongo Conventions",
               new ConventionPack
            {
                new DelegateClassMapConvention("Set Hangfire Mongo Discriminator", cm => 
                cm.SetDiscriminator(cm.ClassType.Name))
            },
               t => t.FullName.StartsWith("Hangfire.Mongo.Dto"));
            return new HangfireDbContext(_runner.ConnectionString, dbName ?? DefaultDatabaseName);
        }

        public static void DropDatabase()
        {
            var client = new MongoClient(_runner.ConnectionString);
            client.DropDatabase(DefaultDatabaseName);
        }
        

        public ConnectionUtils(IMessageSink messageSink) : base(messageSink)
        {
            var homePath = (Environment.OSVersion.Platform == PlatformID.Unix || 
                            Environment.OSVersion.Platform == PlatformID.MacOSX)
                ? Environment.GetEnvironmentVariable("HOME")
                : Environment.ExpandEnvironmentVariables("%HOMEDRIVE%%HOMEPATH%");
            _runner = MongoDbRunner.Start(
                dataDirectory: Path.Combine(homePath, "db"),
                singleNodeReplSet: true,
                logger: new NullLogger<MongoDbRunner>());
            DisposalTracker.Add(_runner);
        }
    }
#pragma warning restore 1591
}
