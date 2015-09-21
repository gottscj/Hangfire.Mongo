using Hangfire.Mongo.Database;
using Hangfire.Mongo.Dto;
using Hangfire.Mongo.MongoUtils;
using Hangfire.Mongo.PersistentJobQueue.Mongo;
using Hangfire.Mongo.Tests.Utils;
using System;
using System.Linq;
using System.Threading;
using Hangfire.Mongo.Helpers;
using MongoDB.Bson;
using MongoDB.Driver;
using Xunit;

namespace Hangfire.Mongo.Tests
{
#pragma warning disable 1591
    [Collection("Database")]
    public class MongoJobQueueFacts
    {
        private static readonly string[] DefaultQueues = { "default" };

        [Fact]
        public void Ctor_ThrowsAnException_WhenConnectionIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(
                () => new MongoJobQueue(null, new MongoStorageOptions()));

            Assert.Equal("connection", exception.ParamName);
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenOptionsValueIsNull()
        {
            UseConnection(connection =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                   () => new MongoJobQueue(connection, null));

                Assert.Equal("options", exception.ParamName);
            });
        }

        [Fact, CleanDatabase]
        public void Dequeue_ShouldThrowAnException_WhenQueuesCollectionIsNull()
        {
            UseConnection(connection =>
            {
                var queue = CreateJobQueue(connection);

                var exception = Assert.Throws<ArgumentNullException>(
                    () => queue.Dequeue(null, CreateTimingOutCancellationToken()));

                Assert.Equal("queues", exception.ParamName);
            });
        }

        [Fact, CleanDatabase]
        public void Dequeue_ShouldThrowAnException_WhenQueuesCollectionIsEmpty()
        {
            UseConnection(connection =>
            {
                var queue = CreateJobQueue(connection);

                var exception = Assert.Throws<ArgumentException>(
                    () => queue.Dequeue(new string[0], CreateTimingOutCancellationToken()));

                Assert.Equal("queues", exception.ParamName);
            });
        }

        [Fact]
        public void Dequeue_ThrowsOperationCanceled_WhenCancellationTokenIsSetAtTheBeginning()
        {
            UseConnection(connection =>
            {
                var cts = new CancellationTokenSource();
                cts.Cancel();
                var queue = CreateJobQueue(connection);

                Assert.Throws<OperationCanceledException>(() => queue.Dequeue(DefaultQueues, cts.Token));
            });
        }

        [Fact, CleanDatabase]
        public void Dequeue_ShouldWaitIndefinitely_WhenThereAreNoJobs()
        {
            UseConnection(connection =>
            {
                var cts = new CancellationTokenSource(200);
                var queue = CreateJobQueue(connection);

                Assert.Throws<OperationCanceledException>(() => queue.Dequeue(DefaultQueues, cts.Token));
            });
        }

        [Fact, CleanDatabase]
        public void Dequeue_ShouldFetchAJob_FromTheSpecifiedQueue()
        {
            // Arrange
            UseConnection(connection =>
            {
                var jobQueue = new JobQueueDto
                {
                    JobId = 1,
                    Queue = "default"
                };

                AsyncHelper.RunSync(() => connection.JobQueue.InsertOneAsync(jobQueue));

                var id = jobQueue.Id;
                var queue = CreateJobQueue(connection);

                // Act
                MongoFetchedJob payload = (MongoFetchedJob)queue.Dequeue(DefaultQueues, CreateTimingOutCancellationToken());

                // Assert
                Assert.Equal(id, payload.Id);
                Assert.Equal("1", payload.JobId);
                Assert.Equal("default", payload.Queue);
            });
        }

        [Fact, CleanDatabase]
        public void Dequeue_ShouldLeaveJobInTheQueue_ButSetItsFetchedAtValue()
        {
            // Arrange
            UseConnection(connection =>
            {
                var job = new JobDto
                {
                    InvocationData = "",
                    Arguments = "",
                    CreatedAt = connection.GetServerTimeUtc()
                };
                AsyncHelper.RunSync(() => connection.Job.InsertOneAsync(job));

                var jobQueue = new JobQueueDto
                {
                    JobId = job.Id,
                    Queue = "default"
                };
                AsyncHelper.RunSync(() => connection.JobQueue.InsertOneAsync(jobQueue));

                var queue = CreateJobQueue(connection);

                // Act
                var payload = queue.Dequeue(DefaultQueues, CreateTimingOutCancellationToken());

                // Assert
                Assert.NotNull(payload);

                var fetchedAt = AsyncHelper.RunSync(() => connection.JobQueue.Find(Builders<JobQueueDto>.Filter.Eq(_ => _.JobId, int.Parse(payload.JobId))).FirstOrDefaultAsync()).FetchedAt;

                Assert.NotNull(fetchedAt);
                Assert.True(fetchedAt > DateTime.UtcNow.AddMinutes(-1));
            });
        }

        [Fact, CleanDatabase]
        public void Dequeue_ShouldFetchATimedOutJobs_FromTheSpecifiedQueue()
        {
            // Arrange
            UseConnection(connection =>
            {
                var job = new JobDto
                {
                    InvocationData = "",
                    Arguments = "",
                    CreatedAt = connection.GetServerTimeUtc()
                };
                AsyncHelper.RunSync(() => connection.Job.InsertOneAsync(job));

                var jobQueue = new JobQueueDto
                {
                    JobId = job.Id,
                    Queue = "default",
                    FetchedAt = connection.GetServerTimeUtc().AddDays(-1)
                };
                AsyncHelper.RunSync(() => connection.JobQueue.InsertOneAsync(jobQueue));

                var queue = CreateJobQueue(connection);

                // Act
                var payload = queue.Dequeue(DefaultQueues, CreateTimingOutCancellationToken());

                // Assert
                Assert.NotEmpty(payload.JobId);
            });
        }

        [Fact, CleanDatabase]
        public void Dequeue_ShouldSetFetchedAt_OnlyForTheFetchedJob()
        {
            // Arrange
            UseConnection(connection =>
            {
                var job1 = new JobDto
                {
                    InvocationData = "",
                    Arguments = "",
                    CreatedAt = connection.GetServerTimeUtc()
                };
                AsyncHelper.RunSync(() => connection.Job.InsertOneAsync(job1));

                var job2 = new JobDto
                {
                    InvocationData = "",
                    Arguments = "",
                    CreatedAt = connection.GetServerTimeUtc()
                };
                AsyncHelper.RunSync(() => connection.Job.InsertOneAsync(job2));

                AsyncHelper.RunSync(() => connection.JobQueue.InsertOneAsync(new JobQueueDto
                {
                    JobId = job1.Id,
                    Queue = "default"
                }));

                AsyncHelper.RunSync(() => connection.JobQueue.InsertOneAsync(new JobQueueDto
                {
                    JobId = job2.Id,
                    Queue = "default"
                }));

                var queue = CreateJobQueue(connection);

                // Act
                var payload = queue.Dequeue(DefaultQueues, CreateTimingOutCancellationToken());

                // Assert
                var otherJobFetchedAt = AsyncHelper.RunSync(() => connection.JobQueue.Find(Builders<JobQueueDto>.Filter.Ne(_ => _.JobId, int.Parse(payload.JobId))).FirstOrDefaultAsync()).FetchedAt;

                Assert.Null(otherJobFetchedAt);
            });
        }

        [Fact, CleanDatabase]
        public void Dequeue_ShouldFetchJobs_OnlyFromSpecifiedQueues()
        {
            UseConnection(connection =>
            {
                var job1 = new JobDto
                {
                    InvocationData = "",
                    Arguments = "",
                    CreatedAt = connection.GetServerTimeUtc()
                };
                AsyncHelper.RunSync(() => connection.Job.InsertOneAsync(job1));

                AsyncHelper.RunSync(() => connection.JobQueue.InsertOneAsync(new JobQueueDto
                {
                    JobId = job1.Id,
                    Queue = "critical"
                }));


                var queue = CreateJobQueue(connection);

                Assert.Throws<OperationCanceledException>(() => queue.Dequeue(DefaultQueues, CreateTimingOutCancellationToken()));
            });
        }

        [Fact, CleanDatabase]
        public void Dequeue_ShouldFetchJobs_FromMultipleQueues()
        {
            UseConnection(connection =>
            {
                var job1 = new JobDto
                {
                    InvocationData = "",
                    Arguments = "",
                    CreatedAt = connection.GetServerTimeUtc()
                };
                AsyncHelper.RunSync(() => connection.Job.InsertOneAsync(job1));

                var job2 = new JobDto
                {
                    InvocationData = "",
                    Arguments = "",
                    CreatedAt = connection.GetServerTimeUtc()
                };
                AsyncHelper.RunSync(() => connection.Job.InsertOneAsync(job2));

                AsyncHelper.RunSync(() => connection.JobQueue.InsertOneAsync(new JobQueueDto
                {
                    JobId = job1.Id,
                    Queue = "critical"
                }));

                AsyncHelper.RunSync(() => connection.JobQueue.InsertOneAsync(new JobQueueDto
                {
                    JobId = job2.Id,
                    Queue = "default"
                }));

                var queue = CreateJobQueue(connection);

                var critical = (MongoFetchedJob)queue.Dequeue(
                    new[] { "critical", "default" },
                    CreateTimingOutCancellationToken());

                Assert.NotNull(critical.JobId);
                Assert.Equal("critical", critical.Queue);

                var @default = (MongoFetchedJob)queue.Dequeue(
                    new[] { "critical", "default" },
                    CreateTimingOutCancellationToken());

                Assert.NotNull(@default.JobId);
                Assert.Equal("default", @default.Queue);
            });
        }

        [Fact, CleanDatabase]
        public void Enqueue_AddsAJobToTheQueue()
        {
            UseConnection(connection =>
            {
                var queue = CreateJobQueue(connection);

                queue.Enqueue("default", "1");

                var record = AsyncHelper.RunSync(() => connection.JobQueue.Find(new BsonDocument()).ToListAsync()).Single();
                Assert.Equal("1", record.JobId.ToString());
                Assert.Equal("default", record.Queue);
                Assert.Null(record.FetchedAt);
            });
        }

        private static CancellationToken CreateTimingOutCancellationToken()
        {
            var source = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            return source.Token;
        }

        private static MongoJobQueue CreateJobQueue(HangfireDbContext connection)
        {
            return new MongoJobQueue(connection, new MongoStorageOptions());
        }

        private static void UseConnection(Action<HangfireDbContext> action)
        {
            using (var connection = ConnectionUtils.CreateConnection())
            {
                action(connection);
            }
        }
    }
#pragma warning restore 1591
}