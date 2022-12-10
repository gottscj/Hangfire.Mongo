using System;
using System.Linq;
using Hangfire.Mongo.Database;
using Hangfire.Mongo.Dto;
using Hangfire.Mongo.Tests.Utils;
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
        private MongoStorageOptions _mongoStorageOptions = new MongoStorageOptions();
        private readonly DateTime _fetchedAt = DateTime.UtcNow;
        private readonly HangfireDbContext _dbContext = ConnectionUtils.CreateDbContext();

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

        [Fact, CleanDatabase]
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

        [Fact, CleanDatabase]
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

        [Fact, CleanDatabase]
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

        [Fact, CleanDatabase]
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

        private ObjectId CreateJobQueueRecord(HangfireDbContext connection, ObjectId jobId, string queue, DateTime? fetchedAt)
        {
            var jobQueue = new JobDto
            {
                Id = jobId,
                Queue = queue,
                FetchedAt = fetchedAt
            };

            connection.JobGraph.InsertOne(jobQueue.Serialize());

            return jobQueue.Id;
        }
    }
#pragma warning restore 1591
}