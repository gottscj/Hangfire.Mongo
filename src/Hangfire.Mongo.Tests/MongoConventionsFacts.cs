using System;
using System.Collections.Concurrent;
using System.Threading;
using Hangfire.Mongo.Migration.Strategies;
using Hangfire.Mongo.Migration.Strategies.Backup;
using Hangfire.Mongo.Tests.Utils;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Conventions;
using MongoDB.Driver;
using Xunit;

namespace Hangfire.Mongo.Tests
{

    public class TestJob
    {
        public static AutoResetEvent Signal { get; } = new AutoResetEvent(false);

        public void SetSignal()
        {
            Signal.Set();
        }
    }
    [Collection("Database")]
    public class MongoConventionsFacts
    {
        [Fact, CleanDatabase(false)]
        public void Conventions_UsesOwnConventionsForDtoNameSpace_WhenCamelCaseIsRegistered()
        {
            // ARRANGE
            var conventionPack = new ConventionPack { new CamelCaseElementNameConvention() };
            ConventionRegistry.Register("camelCase", conventionPack, t => true);

            // ACT
            // This line will throw during migration if camelCase is active for the Dto namespace.
            var connection = ConnectionUtils.CreateDbContext();

            // ASSERT
            Assert.NotNull(connection);
        }
        
        [Fact, CleanDatabase(false)]
        public void CamelCaseConvention_HangfireMongoDtos_StillInPascal()
        {
            // ARRANGE
            var mongoStorage = new MongoStorage(
                MongoClientSettings.FromConnectionString(ConnectionUtils.GetConnectionString()),
                ConnectionUtils.GetDatabaseName(), new MongoStorageOptions
                {
                    MigrationOptions = new MongoMigrationOptions
                    {
                        MigrationStrategy = new DropMongoMigrationStrategy(),
                        BackupStrategy = new NoneMongoBackupStrategy()
                    }
                }
            );

            var dbContext = ConnectionUtils.CreateDbContext();
            JobStorage.Current = mongoStorage;

            bool jobScheduled;

            var conventionPack = new ConventionPack {new CamelCaseElementNameConvention()};
            ConventionRegistry.Register("CamelCase", conventionPack, t => true);
            
            // ACT
            using (new BackgroundJobServer())
            {
                BackgroundJob.Enqueue<TestJob>(j => j.SetSignal());
                jobScheduled = TestJob.Signal.WaitOne(TimeSpan.FromSeconds(10));
            }

            // ASSERT
            var jobGraphCollectionName = dbContext.JobGraph.CollectionNamespace.CollectionName;
            var jobDto = dbContext
                .Database
                .GetCollection<BsonDocument>(jobGraphCollectionName)
                .Find(new BsonDocument("expireAt", new BsonDocument("$exists", true)))
                .FirstOrDefault();

            Assert.Null(jobDto);
            Assert.True(jobScheduled, "Expected job to be scheduled");
        }

    }

}
