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
                () => new MongoFetchedJob(null, _mongoStorageOptions, _fetchedAt, ObjectId.GenerateNewId(), JobId, Queue));

            Assert.Equal("db", exception.ParamName);
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenQueueIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(
                () => new MongoFetchedJob(_dbContext, _mongoStorageOptions, _fetchedAt, ObjectId.GenerateNewId(), JobId, null));

            Assert.Equal("queue", exception.ParamName);
        }

        [Fact]
        public void Ctor_CorrectlySets_AllInstanceProperties()
        {
            var fetchedJob = new MongoFetchedJob(_dbContext, _mongoStorageOptions, _fetchedAt, ObjectId.GenerateNewId(), JobId, Queue);

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
            var processingJob = new MongoFetchedJob(_dbContext, _mongoStorageOptions, _fetchedAt, id, jobId, queue);

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

            var fetchedJob = new MongoFetchedJob(_dbContext, _mongoStorageOptions, _fetchedAt, ObjectId.GenerateNewId(), ObjectId.GenerateNewId(999), "default");

            // Act
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
            var processingJob = new MongoFetchedJob(_dbContext, _mongoStorageOptions, _fetchedAt, id, jobId, queue);

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
            var processingJob = new MongoFetchedJob(_dbContext, _mongoStorageOptions, _fetchedAt, id, jobId, queue);

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
            var job = new MongoFetchedJob(_dbContext, options, initialFetchedAt, id, jobId, queue);
            // job runs for 2s, Heartbeat updates job
            Thread.Sleep(TimeSpan.FromSeconds(2));
            job.Dispose();
            
            // Assert
            Assert.True(job.FetchedAt > initialFetchedAt, "Expected job FetchedAt field to be updated");
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
                StateName = stateName
            };

            connection.JobGraph.InsertOne(job.Serialize());

            return job.Id;
        }
    }
#pragma warning restore 1591
}