using System;
using System.Threading;
using Hangfire.Mongo.Database;
using Hangfire.Mongo.Dto;
using Hangfire.Mongo.Tests.Utils;
using Hangfire.States;
using MongoDB.Bson;
using MongoDB.Driver;
using Xunit;

namespace Hangfire.Mongo.Tests
{
#pragma warning disable 1591
    [Collection("Database")]
    public class AsyncMongoFetchedJobFacts
    {
        private const string Queue = "default";
        private static readonly TimeSpan HeartbeatTimeout = TimeSpan.FromSeconds(1);

        private readonly MongoStorageOptions _options = new MongoStorageOptions
        {
            SlidingInvisibilityTimeout = HeartbeatTimeout
        };
        private readonly string _fetchToken = Guid.NewGuid().ToString("N");
        private readonly DateTime _initialFetchedAt = DateTime.UtcNow.AddMinutes(-10);
        private readonly HangfireDbContext _dbContext;

        public AsyncMongoFetchedJobFacts(MongoIntegrationTestFixture fixture)
        {
            fixture.CleanDatabase();
            _dbContext = fixture.CreateDbContext();
        }

        [Fact]
        public void Heartbeat_UpdatesFetchedAt_WhileJobIsProcessing()
        {
            var id = CreateFetchedJobRecord(ProcessingState.StateName);

            using var job = CreateFetchedJob(id, _options);

            Assert.True(
                Wait.Until(() => GetFetchedAt(id) > _initialFetchedAt.AddMinutes(5)),
                "Expected heartbeat to update FetchedAt");
        }

        [Fact]
        public void Heartbeat_LeavesDocumentIntact_WhenLeaseWasStolen()
        {
            var id = CreateFetchedJobRecord(ProcessingState.StateName);
            var thiefToken = Guid.NewGuid().ToString("N");
            SetFetchToken(id, thiefToken);

            using (CreateFetchedJob(id, _options))
            {
                Thread.Sleep(HeartbeatTimeout * 2);

                var doc = GetJob(id);
                Assert.Equal(thiefToken, doc[nameof(JobDto.FetchToken)].AsString);
                Assert.Equal(_initialFetchedAt, doc[nameof(JobDto.FetchedAt)].ToUniversalTime(), TimeSpan.FromSeconds(1));
            }
        }

        [Fact]
        public void Heartbeat_DoesNotRun_WhenSlidingInvisibilityTimeoutIsNull()
        {
            var id = CreateFetchedJobRecord(ProcessingState.StateName);
            var options = new MongoStorageOptions { SlidingInvisibilityTimeout = null };

            using (CreateFetchedJob(id, options))
            {
                Thread.Sleep(HeartbeatTimeout);

                Assert.Equal(_initialFetchedAt, GetFetchedAt(id), TimeSpan.FromSeconds(1));
            }
        }

        [Fact]
        public void Dispose_StopsHeartbeat()
        {
            var id = CreateFetchedJobRecord(ProcessingState.StateName);
            var job = CreateFetchedJob(id, _options);
            Assert.True(Wait.Until(() => GetFetchedAt(id) > _initialFetchedAt.AddMinutes(5)), "Expected heartbeat to run");

            job.Dispose();
            // put the job back into the state the heartbeat looks for, as if it was fetched again
            _dbContext.JobGraph.UpdateOne(new BsonDocument("_id", id), new BsonDocument("$set", new BsonDocument
            {
                [nameof(JobDto.FetchedAt)] = _initialFetchedAt,
                [nameof(JobDto.FetchToken)] = _fetchToken
            }));
            Thread.Sleep(HeartbeatTimeout * 2);

            Assert.Equal(_initialFetchedAt, GetFetchedAt(id), TimeSpan.FromSeconds(1));
        }

        [Fact]
        public void Dispose_RequeuesJob_WhenNotRemovedFromQueue()
        {
            var id = CreateFetchedJobRecord(ProcessingState.StateName);
            var job = CreateFetchedJob(id, _options);

            job.Dispose();

            var doc = GetJob(id);
            Assert.Equal(Queue, doc[nameof(JobDto.Queue)].AsString);
            Assert.Equal(BsonNull.Value, doc[nameof(JobDto.FetchedAt)]);
            Assert.Equal(BsonNull.Value, doc[nameof(JobDto.FetchToken)]);
        }

        [Fact]
        public void RemoveFromQueue_ClearsQueue_AndDisposeDoesNotRequeue()
        {
            var id = CreateFetchedJobRecord(ProcessingState.StateName);
            var job = CreateFetchedJob(id, _options);

            job.RemoveFromQueue();
            job.Dispose();

            var doc = GetJob(id);
            Assert.Equal(BsonNull.Value, doc[nameof(JobDto.Queue)]);
            Assert.Equal(BsonNull.Value, doc[nameof(JobDto.FetchedAt)]);
            Assert.Equal(BsonNull.Value, doc[nameof(JobDto.FetchToken)]);
        }

        [Fact]
        public void Dispose_CanBeCalledMoreThanOnce()
        {
            var id = CreateFetchedJobRecord(ProcessingState.StateName);
            var job = CreateFetchedJob(id, _options);

            job.Dispose();
            job.Dispose();
        }

        private AsyncMongoFetchedJob CreateFetchedJob(ObjectId id, MongoStorageOptions options)
        {
            return new AsyncMongoFetchedJob(_dbContext, options, _initialFetchedAt, _fetchToken, id, id, Queue);
        }

        private ObjectId CreateFetchedJobRecord(string stateName)
        {
            var job = new JobDto
            {
                Id = ObjectId.GenerateNewId(),
                Queue = Queue,
                FetchedAt = _initialFetchedAt,
                FetchToken = _fetchToken,
                StateName = stateName
            };
            _dbContext.JobGraph.InsertOne(job.Serialize());
            return job.Id;
        }

        private void SetFetchToken(ObjectId id, string fetchToken)
        {
            _dbContext.JobGraph.UpdateOne(
                new BsonDocument("_id", id),
                new BsonDocument("$set", new BsonDocument(nameof(JobDto.FetchToken), fetchToken)));
        }

        private BsonDocument GetJob(ObjectId id)
        {
            return _dbContext.JobGraph.Find(new BsonDocument("_id", id)).Single();
        }

        private DateTime GetFetchedAt(ObjectId id)
        {
            return GetJob(id)[nameof(JobDto.FetchedAt)].ToUniversalTime();
        }
    }
#pragma warning restore 1591
}
