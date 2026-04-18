using System;
using System.Linq;
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
    public class MongoFetchedJobFacts
    {
        private static readonly ObjectId JobId = ObjectId.GenerateNewId();
        private const string Queue = "queue";
        private readonly MongoStorageOptions _mongoStorageOptions = new MongoStorageOptions();
        private readonly DateTime _fetchedAt = DateTime.UtcNow;
        private readonly string _fetchToken = Guid.NewGuid().ToString("N");
        private readonly HangfireDbContext _dbContext;

        public MongoFetchedJobFacts(MongoIntegrationTestFixture fixture)
        {
            fixture.CleanDatabase();
            _dbContext = fixture.CreateDbContext();
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenConnectionIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(
                () => new MongoFetchedJob(null, _mongoStorageOptions, _fetchedAt, _fetchToken, ObjectId.GenerateNewId(), JobId, Queue));

            Assert.Equal("db", exception.ParamName);
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenQueueIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(
                () => new MongoFetchedJob(_dbContext, _mongoStorageOptions, _fetchedAt, _fetchToken, ObjectId.GenerateNewId(), JobId, null));

            Assert.Equal("queue", exception.ParamName);
        }

        [Fact]
        public void Ctor_CorrectlySets_AllInstanceProperties()
        {
            var fetchedJob = new MongoFetchedJob(_dbContext, _mongoStorageOptions, _fetchedAt, _fetchToken, ObjectId.GenerateNewId(), JobId, Queue);

            Assert.Equal(JobId.ToString(), fetchedJob.JobId);
            Assert.Equal(Queue, fetchedJob.Queue);
        }

        [Fact]
        public void RemoveFromQueue_ReallyDeletesTheJobFromTheQueue()
        {
            // Arrange
            var queue = "default";
            var jobId = ObjectId.GenerateNewId();
            var id = CreateJobQueueRecord(_dbContext, jobId, queue, _fetchedAt);
            var processingJob = new MongoFetchedJob(_dbContext, _mongoStorageOptions, _fetchedAt, _fetchToken, id, jobId, queue);

            // Act
            processingJob.RemoveFromQueue();

            // Assert
            var filter = new BsonDocument
            {
                ["_t"] = nameof(JobDto),
                [nameof(JobDto.Queue)] = new BsonDocument("$ne", BsonNull.Value)
            };
            var count = _dbContext.JobGraph.Count(filter);
            Assert.Equal(0, count);
        }

        [Fact]
        public void RemoveFromQueue_DoesNotDelete_UnrelatedJobs()
        {
            // Arrange
            CreateJobQueueRecord(_dbContext, ObjectId.GenerateNewId(1), "default", _fetchedAt);
            CreateJobQueueRecord(_dbContext, ObjectId.GenerateNewId(2), "critical", _fetchedAt);
            CreateJobQueueRecord(_dbContext, ObjectId.GenerateNewId(3), "default", _fetchedAt);

            var fetchedJob = new MongoFetchedJob(_dbContext, _mongoStorageOptions, _fetchedAt, _fetchToken, ObjectId.GenerateNewId(), ObjectId.GenerateNewId(999), "default");

            // Act — CAS on (_id, FetchToken) never matches a random id, so the ack silently logs
            // a warning and leaves the three unrelated documents intact.
            fetchedJob.RemoveFromQueue();

            // Assert
            var filter = new BsonDocument
            {
                ["_t"] = nameof(JobDto),
                [nameof(JobDto.Queue)] = new BsonDocument("$ne", BsonNull.Value)
            };
            var count = _dbContext.JobGraph.Count(filter);
            Assert.Equal(3, count);
        }

        [Fact]
        public void Requeue_SetsFetchedAtValueToNull()
        {
            // Arrange
            var queue = "default";
            var jobId = ObjectId.GenerateNewId();
            var id = CreateJobQueueRecord(_dbContext, jobId, queue, _fetchedAt);
            var processingJob = new MongoFetchedJob(_dbContext, _mongoStorageOptions, _fetchedAt, _fetchToken, id, jobId, queue);

            // Act
            processingJob.Requeue();

            // Assert
            var record = new JobDto(
                _dbContext.JobGraph.Find(new BsonDocument("_t", nameof(JobDto))).ToList().Single());
            Assert.Null(record.FetchedAt);
        }

        [Fact]
        public void Dispose_SetsFetchedAtValueToNull_IfThereWereNoCallsToComplete()
        {
            // Arrange
            var queue = "default";
            var jobId = ObjectId.GenerateNewId();
            var id = CreateJobQueueRecord(_dbContext, jobId, queue, _fetchedAt);
            var processingJob = new MongoFetchedJob(_dbContext, _mongoStorageOptions, _fetchedAt, _fetchToken, id, jobId, queue);

            // Act
            processingJob.Dispose();

            // Assert
            var record = new JobDto(
                _dbContext.JobGraph.Find(new BsonDocument("_t", nameof(JobDto))).ToList().Single());
            Assert.Null(record.FetchedAt);
        }
        
        [Fact]
        public void Heartbeat_LonRunningJob_UpdatesFetchedAt()
        {
            // Arrange
            // time out job after 1s
            var options = new MongoStorageOptions() {SlidingInvisibilityTimeout = TimeSpan.FromSeconds(1)};
            var queue = "default";
            var jobId = ObjectId.GenerateNewId();
            var id = CreateJobQueueRecord(_dbContext, jobId, queue, _fetchedAt, ProcessingState.StateName);
            var initialFetchedAt = DateTime.UtcNow;

            // Act
            var job = new MongoFetchedJob(_dbContext, options, initialFetchedAt, _fetchToken, id, jobId, queue);
            // job runs for 2s, Heartbeat updates job
            Thread.Sleep(TimeSpan.FromSeconds(2));
            job.Dispose();

            // Assert
            Assert.True(job.FetchedAt > initialFetchedAt, "Expected job FetchedAt field to be updated");
        }

        [Fact]
        public void RemoveFromQueue_LeavesDocumentIntact_WhenLeaseWasStolen()
        {
            // Arrange — worker A fetches the job (token = _fetchToken).
            var queue = "default";
            var jobId = ObjectId.GenerateNewId();
            var id = CreateJobQueueRecord(_dbContext, jobId, queue, _fetchedAt);
            var workerA = new MongoFetchedJob(_dbContext, _mongoStorageOptions, _fetchedAt, _fetchToken, id, jobId, queue);

            // Worker B takes over after invisibility timeout — DB token is replaced.
            var thiefToken = Guid.NewGuid().ToString("N");
            _dbContext.JobGraph.UpdateOne(
                new BsonDocument("_id", id),
                new BsonDocument("$set", new BsonDocument(nameof(JobDto.FetchToken), thiefToken)));

            // Act — ack is a no-op because the CAS does not match; a warning is logged (not asserted here).
            workerA.RemoveFromQueue();

            // Assert — new owner's lease is preserved in the document.
            var doc = _dbContext.JobGraph.Find(new BsonDocument("_id", id)).Single();
            Assert.Equal(queue, doc[nameof(JobDto.Queue)].AsString);
            Assert.Equal(thiefToken, doc[nameof(JobDto.FetchToken)].AsString);
        }

        [Fact]
        public void TransactionRemoveFromQueue_FetchedJobOverload_ClearsOwnershipFields()
        {
            // Arrange — simulate an actively fetched job.
            var queue = "default";
            var jobId = ObjectId.GenerateNewId();
            var id = CreateJobQueueRecord(_dbContext, jobId, queue, _fetchedAt);
            var fetched = new MongoFetchedJob(_dbContext, _mongoStorageOptions, _fetchedAt, _fetchToken, id, jobId, queue);

            // Act — bundled ack path used by Hangfire.Core when Transaction.RemoveFromQueue feature is advertised.
            using (var tx = new MongoWriteOnlyTransaction(_dbContext, _mongoStorageOptions))
            {
                tx.RemoveFromQueue(fetched);
                tx.Commit();
            }

            // Assert — queue, fetched-at and fetch-token are all nulled.
            var doc = _dbContext.JobGraph.Find(new BsonDocument("_id", id)).Single();
            Assert.Equal(BsonNull.Value, doc[nameof(JobDto.Queue)]);
            Assert.Equal(BsonNull.Value, doc[nameof(JobDto.FetchToken)]);
            Assert.Equal(BsonNull.Value, doc[nameof(JobDto.FetchedAt)]);
        }

        private ObjectId CreateJobQueueRecord(
            HangfireDbContext connection,
            ObjectId jobId,
            string queue,
            DateTime? fetchedAt,
            string stateName = null)
        {
            var job = new JobDto
            {
                Id = jobId,
                Queue = queue,
                FetchedAt = fetchedAt,
                FetchToken = _fetchToken,
                StateName = stateName
            };

            connection.JobGraph.InsertOne(job.Serialize());

            return job.Id;
        }
    }
#pragma warning restore 1591
}