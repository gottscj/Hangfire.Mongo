using System;
using System.Collections.Generic;
using System.Threading;
using Hangfire.Mongo.Database;
using Hangfire.Mongo.Dto;
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
        public static List<string> JobIds { get; } = new List<string>();
        public void AddId(string id)
        {
            JobIds.Add(id);
        }

        public void AddAndSignal(string id)
        {
            AddId(id);
            SetSignal();
        }

        public void SetSignal()
        {
            Signal.Set();
        }
    }

    public class MongoRunFixture : IDisposable
    {
        private readonly BackgroundJobServer _server;
        public HangfireDbContext DbContext { get; }

        public MongoRunFixture(MongoIntegrationTestFixture fixture)
        {
            var databaseName = "Mongo-Hangfire-CamelCase";
            var context = fixture.CreateDbContext(databaseName);
            DbContext = context;
            // Make sure we start from scratch
            context.Database.Client.DropDatabase(databaseName);

            var storageOptions = new MongoStorageOptions
            {
                MigrationOptions = new MongoMigrationOptions
                {
                    MigrationStrategy = new DropMongoMigrationStrategy(),
                    BackupStrategy = new NoneMongoBackupStrategy()
                }
            };

            JobStorage.Current = fixture.CreateStorage(storageOptions, databaseName);

            var conventionPack = new ConventionPack {new CamelCaseElementNameConvention()};
            ConventionRegistry.Register("CamelCase", conventionPack, t => true);

            _server = new BackgroundJobServer(new BackgroundJobServerOptions
                {SchedulePollingInterval = TimeSpan.FromMilliseconds(100)});
        }

        public void Dispose()
        {
            _server?.Dispose();
        }
    }


    [Collection("Database")]
    public class MongoRunServerFacts : IClassFixture<MongoRunFixture>
    {
        private readonly MongoRunFixture _fixture;

        public MongoRunServerFacts(MongoRunFixture fixture)
        {
            _fixture = fixture;
        }

        [Fact]
        public void CamelCaseConvention_HangfireMongoDtos_StillInPascal()
        {
            // ARRANGE

            // ACT
            BackgroundJob.Enqueue<TestJob>(j => j.SetSignal());
            var jobScheduled = TestJob.Signal.WaitOne(TimeSpan.FromSeconds(20));

            // ASSERT
            var jobGraphCollectionName = _fixture.DbContext.JobGraph.CollectionNamespace.CollectionName;
            var jobDto = _fixture.DbContext
                .Database
                .GetCollection<BsonDocument>(jobGraphCollectionName)
                .Find(new BsonDocument("expireAt", new BsonDocument("$exists", true)))
                .FirstOrDefault();

            Assert.Null(jobDto);
            Assert.True(jobScheduled, "Expected job to be scheduled");
        }

        [Fact]
        public void ContinueWith_Executed_Success()
        {
            // ARRANGE
            // ACT
            var parentId1 = BackgroundJob.Enqueue<TestJob>(j => j.AddId("parent"));
            var parentId2 = BackgroundJob.ContinueWith<TestJob>(parentId1, j => j.AddId(parentId1));
            BackgroundJob.ContinueWith<TestJob>(parentId2, j => j.AddAndSignal(parentId2));
            var signalled = TestJob.Signal.WaitOne(20000);

            // ASSERT
            Assert.True(signalled, "not signalled");
            Assert.Equal(3, TestJob.JobIds.Count);
            var parent = TestJob.JobIds[0];
            var parentId1Expected = TestJob.JobIds[1];
            var parentId2Expected = TestJob.JobIds[2]
                .Replace("\n", "")
                .Replace("\r", "");
            Assert.Equal("parent", parent);
            Assert.Equal(parentId1, parentId1Expected);
            Assert.Equal(parentId2, parentId2Expected);

        }

        [Fact]
        public void Enqueue_SuccessfulJob_ClearsQueueAndFetchTokenViaTransactionalAck()
        {
            // ARRANGE
            var jobGraphCollectionName = _fixture.DbContext.JobGraph.CollectionNamespace.CollectionName;
            var jobGraph = _fixture.DbContext.Database.GetCollection<BsonDocument>(jobGraphCollectionName);

            // ACT
            var jobId = BackgroundJob.Enqueue<TestJob>(j => j.SetSignal());
            var signalled = TestJob.Signal.WaitOne(TimeSpan.FromSeconds(20));

            // The worker commits the terminal state transition and the queue ack in one bulk
            // (enabled by JobStorageFeatures.Transaction.RemoveFromQueue). Poll briefly to let
            // the commit land after the job callback returned.
            BsonDocument persisted = null;
            var deadline = DateTime.UtcNow.AddSeconds(5);
            while (DateTime.UtcNow < deadline)
            {
                persisted = jobGraph.Find(new BsonDocument("_id", ObjectId.Parse(jobId))).FirstOrDefault();
                if (persisted != null && persisted[nameof(JobDto.Queue)] == BsonNull.Value)
                {
                    break;
                }
                Thread.Sleep(50);
            }

            // ASSERT
            Assert.True(signalled, "job did not run");
            Assert.NotNull(persisted);
            Assert.Equal(BsonNull.Value, persisted[nameof(JobDto.Queue)]);
            Assert.Equal(BsonNull.Value, persisted[nameof(JobDto.FetchToken)]);
            Assert.Equal(BsonNull.Value, persisted[nameof(JobDto.FetchedAt)]);
        }
    }

}
