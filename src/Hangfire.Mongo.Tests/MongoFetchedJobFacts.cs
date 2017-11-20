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
    [Collection("Database")]
    public class MongoFetchedJobFacts
    {
        private static readonly ObjectId JobId = ObjectId.GenerateNewId();
        private const string Queue = "queue";


        [Fact]
        public void Ctor_ThrowsAnException_WhenConnectionIsNull()
        {
            ConnectionUtils.UseConnection(database =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => new MongoFetchedJob(null, ObjectId.GenerateNewId(), JobId, Queue));

                Assert.Equal("database", exception.ParamName);
            });
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenQueueIsNull()
        {
            ConnectionUtils.UseConnection(database =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => new MongoFetchedJob(database, ObjectId.GenerateNewId(), JobId, null));

                Assert.Equal("queue", exception.ParamName);
            });
        }

        [Fact]
        public void Ctor_CorrectlySets_AllInstanceProperties()
        {
            ConnectionUtils.UseConnection(database =>
            {
                var fetchedJob = new MongoFetchedJob(database, ObjectId.GenerateNewId(), JobId, Queue);

                Assert.Equal(JobId.ToString(), fetchedJob.JobId);
                Assert.Equal(Queue, fetchedJob.Queue);
            });
        }

        [Fact, CleanDatabase]
        public void RemoveFromQueue_ReallyDeletesTheJobFromTheQueue()
        {
            ConnectionUtils.UseConnection(database =>
            {
                // Arrange
                var queue = "default";
                var jobId = ObjectId.GenerateNewId();
                var id = CreateJobQueueRecord(database, jobId, queue);
                var processingJob = new MongoFetchedJob(database, id, jobId, queue);

                // Act
                processingJob.RemoveFromQueue();

                // Assert
                var count = database.JobQueue.Count(new BsonDocument());
                Assert.Equal(0, count);
            });
        }

        [Fact, CleanDatabase]
        public void RemoveFromQueue_DoesNotDelete_UnrelatedJobs()
        {
            ConnectionUtils.UseConnection(database =>
            {
                // Arrange
                CreateJobQueueRecord(database, ObjectId.GenerateNewId(1), "default");
                CreateJobQueueRecord(database, ObjectId.GenerateNewId(2), "critical");
                CreateJobQueueRecord(database, ObjectId.GenerateNewId(3), "default");

                var fetchedJob = new MongoFetchedJob(database, ObjectId.GenerateNewId(), ObjectId.GenerateNewId(999), "default");

                // Act
                fetchedJob.RemoveFromQueue();

                // Assert
                var count = database.JobQueue.Count(new BsonDocument());
                Assert.Equal(3, count);
            });
        }

        [Fact, CleanDatabase]
        public void Requeue_SetsFetchedAtValueToNull()
        {
            ConnectionUtils.UseConnection(database =>
            {
                // Arrange
                var queue = "default";
                var jobId = ObjectId.GenerateNewId();
                var id = CreateJobQueueRecord(database, jobId, queue);
                var processingJob = new MongoFetchedJob(database, id, jobId, queue);

                // Act
                processingJob.Requeue();

                // Assert
                var record = database.JobQueue.Find(new BsonDocument()).ToList().Single();
                Assert.Null(record.FetchedAt);
            });
        }

        [Fact, CleanDatabase]
        public void Dispose_SetsFetchedAtValueToNull_IfThereWereNoCallsToComplete()
        {
            ConnectionUtils.UseConnection(database =>
            {
                // Arrange
                var queue = "default";
                var jobId = ObjectId.GenerateNewId();
                var id = CreateJobQueueRecord(database, jobId, queue);
                var processingJob = new MongoFetchedJob(database, id, jobId, queue);

                // Act
                processingJob.Dispose();

                // Assert
                var record = database.JobQueue.Find(new BsonDocument()).ToList().Single();
                Assert.Null(record.FetchedAt);
            });
        }

        private static ObjectId CreateJobQueueRecord(HangfireDbContext database, ObjectId jobId, string queue)
        {
            var jobQueue = new JobQueueDto
            {
                Id = ObjectId.GenerateNewId(),
                JobId = jobId,
                Queue = queue,
                FetchedAt = DateTime.UtcNow
            };

            database.JobQueue.InsertOne(jobQueue);

            return jobQueue.Id;
        }

    }
}