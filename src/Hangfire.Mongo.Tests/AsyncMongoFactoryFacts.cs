using System;
using System.Threading;
using Hangfire.Mongo.DistributedLock;
using Hangfire.Mongo.Dto;
using Hangfire.Mongo.Migration.Strategies;
using Hangfire.Mongo.Migration.Strategies.Backup;
using Hangfire.Mongo.Tests.Utils;
using Hangfire.States;
using MongoDB.Bson;
using MongoDB.Driver;
using Xunit;

namespace Hangfire.Mongo.Tests
{
#pragma warning disable 1591
    public class AsyncFactoryTestJob
    {
        public static ManualResetEventSlim Signal { get; } = new ManualResetEventSlim();

        public void Run()
        {
            Signal.Set();
        }
    }

    [Collection("Database")]
    public class AsyncMongoFactoryFacts
    {
        private const string DatabaseName = "Hangfire-Mongo-AsyncFactory-Tests";
        private readonly MongoIntegrationTestFixture _fixture;

        public AsyncMongoFactoryFacts(MongoIntegrationTestFixture fixture)
        {
            _fixture = fixture;
        }

        [Fact]
        public void CreateFetchedJob_ReturnsAsyncMongoFetchedJob()
        {
            var dbContext = _fixture.CreateDbContext();
            var id = ObjectId.GenerateNewId();

            using var fetchedJob = new AsyncMongoFactory().CreateFetchedJob(dbContext, new MongoStorageOptions(),
                DateTime.UtcNow, Guid.NewGuid().ToString("N"), id, id, "default");

            Assert.IsType<AsyncMongoFetchedJob>(fetchedJob);
        }

        [Fact]
        public void CreateMongoDistributedLock_ReturnsAsyncMongoDistributedLock_WithHangfirePrefix()
        {
            _fixture.CleanDatabase();
            var dbContext = _fixture.CreateDbContext();

            var distributedLock = new AsyncMongoFactory().CreateMongoDistributedLock("resource1", TimeSpan.Zero,
                dbContext, new MongoStorageOptions());

            Assert.IsType<AsyncMongoDistributedLock>(distributedLock);
            using (distributedLock.AcquireLock())
            {
                var filter = new BsonDocument(nameof(DistributedLockDto.Resource), "Hangfire:resource1");
                Assert.Equal(1, dbContext.DistributedLock.CountDocuments(filter));
            }
        }

        [Fact]
        public void BackgroundJobServer_ProcessesJob_WithAsyncMongoFactory()
        {
            var storage = CreateStorage();
            using var connection = storage.GetConnection();
            using (var distributedLock = connection.AcquireDistributedLock("async-factory", TimeSpan.FromSeconds(5)))
            {
                Assert.IsType<AsyncMongoDistributedLock>(distributedLock);
            }

            AsyncFactoryTestJob.Signal.Reset();
            var client = new BackgroundJobClient(storage);
            string jobId;
            using (new BackgroundJobServer(new BackgroundJobServerOptions
                   {
                       WorkerCount = 1,
                       SchedulePollingInterval = TimeSpan.FromMilliseconds(100)
                   }, storage))
            {
                jobId = client.Enqueue<AsyncFactoryTestJob>(j => j.Run());
                Assert.True(AsyncFactoryTestJob.Signal.Wait(TimeSpan.FromSeconds(20)), "Expected job to run");
                Assert.True(WaitUntil(() => GetJob(jobId).StateName == SucceededState.StateName),
                    "Expected job to succeed");
            }

            var job = GetJob(jobId);
            Assert.Null(job.Queue);
            Assert.Null(job.FetchToken);
        }

        private MongoStorage CreateStorage()
        {
            _fixture.CreateDbContext(DatabaseName).Database.Client.DropDatabase(DatabaseName);
            var options = new MongoStorageOptions
            {
                Factory = new AsyncMongoFactory(),
                QueuePollInterval = TimeSpan.FromMilliseconds(100),
                MigrationOptions = new MongoMigrationOptions
                {
                    MigrationStrategy = new DropMongoMigrationStrategy(),
                    BackupStrategy = new NoneMongoBackupStrategy()
                }
            };
            return _fixture.CreateStorage(options, DatabaseName);
        }

        private JobDto GetJob(string jobId)
        {
            var filter = new BsonDocument("_id", ObjectId.Parse(jobId));
            return new JobDto(_fixture.CreateDbContext(DatabaseName).JobGraph.Find(filter).Single());
        }

        private static bool WaitUntil(Func<bool> condition)
        {
            var deadline = DateTime.UtcNow.AddSeconds(10);
            while (DateTime.UtcNow < deadline)
            {
                if (condition())
                {
                    return true;
                }

                Thread.Sleep(50);
            }

            return condition();
        }
    }
#pragma warning restore 1591
}
