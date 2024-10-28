using System;
using System.Threading;
using System.Threading.Tasks;
using Hangfire.Mongo.Database;
using Hangfire.Mongo.Dto;
using Hangfire.Mongo.Tests.Utils;
using MongoDB.Bson;
using NSubstitute;
using Xunit;

namespace Hangfire.Mongo.Tests
{
    [Collection("Database")]
    public sealed class MongoWatcherFacts : IDisposable
    {
        private readonly HangfireDbContext _dbContext;

        private readonly IJobQueueSemaphore _jobQueueSemaphoreMock;
        private readonly CancellationTokenSource _cts;

        public MongoWatcherFacts(MongoIntegrationTestFixture fixture)
        {
            _dbContext = fixture.CreateDbContext();
            _jobQueueSemaphoreMock = Substitute.For<IJobQueueSemaphore>();
            var watcher = new MongoJobQueueWatcher(
                _dbContext,
                new MongoStorageOptions(),
                _jobQueueSemaphoreMock);

            _cts = new CancellationTokenSource();

            Task.Run(async () =>
            {
                await Task.Yield();
                watcher.Execute(_cts.Token);
            });
            Thread.Sleep(1000);
        }

        public void Dispose()
        {
            _cts.Cancel();
            _cts.Dispose();
        }

        [Fact]
        public void Execute_JobEnqueued_Signaled()
        {
            // ARRANGE
            var signal = new SemaphoreSlim(0,1);
            _jobQueueSemaphoreMock
                .When(m => m.Release("test"))
                .Do(_ => signal.Release());
            var job = new JobDto();

            _dbContext.JobGraph.InsertOne(job.Serialize());

            // ACT
            _dbContext.JobGraph.UpdateOne(new BsonDocument
            {
                ["_id"] = job.Id
            }, new BsonDocument
            {
                ["$set"] = new BsonDocument
                {
                    [nameof(JobDto.Queue)] = "test"
                }
            });
            signal.Wait(20000);

            // ASSERT
            _jobQueueSemaphoreMock.Received(1).Release("test");
        }
    }
}