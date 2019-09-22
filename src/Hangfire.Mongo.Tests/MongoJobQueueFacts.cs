using System;
using System.Threading;
using Hangfire.Mongo.Database;
using Hangfire.Mongo.Dto;
using Hangfire.Mongo.Tests.Utils;
using MongoDB.Bson;
using MongoDB.Driver;
using Moq;
using Xunit;

namespace Hangfire.Mongo.Tests
{
#pragma warning disable 1591
    [Collection("Database")]
    public class MongoJobQueueFacts
    {
        private static readonly string[] DefaultQueues = { "default" };

        private readonly Mock<IJobQueueSemaphore> _jobQueueSemaphoreMock;
        public MongoJobQueueFacts()
        {
            _jobQueueSemaphoreMock = new Mock<IJobQueueSemaphore>(MockBehavior.Strict);
            var queue = "default";
            var timedOut = false;
            _jobQueueSemaphoreMock.Setup(s =>
                    s.WaitAny(DefaultQueues, It.IsAny<CancellationToken>(), It.IsAny<TimeSpan>(), out queue, out timedOut))
                .Returns(true);
        }
        [Fact]
        public void Ctor_ThrowsAnException_WhenDbContextIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(() =>
                new MongoJobFetcher(null, new MongoStorageOptions(), _jobQueueSemaphoreMock.Object));

            Assert.Equal("dbContext", exception.ParamName);
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenOptionsValueIsNull()
        {
            UseConnection(connection =>
            {
                var exception = Assert.Throws<ArgumentNullException>(() =>
                    new MongoJobFetcher(connection, null, _jobQueueSemaphoreMock.Object));

                Assert.Equal("storageOptions", exception.ParamName);
            });
        }

        [Fact, CleanDatabase]
        public void Dequeue_ShouldThrowAnException_WhenQueuesCollectionIsNull()
        {
            UseConnection(connection =>
            {
                var queue = CreateJobQueue(connection);

                var exception = Assert.Throws<ArgumentNullException>(() =>
                    queue.FetchNextJob(null, CreateTimingOutCancellationToken()));

                Assert.Equal("queues", exception.ParamName);
            });
        }

        [Fact, CleanDatabase]
        public void Dequeue_ShouldThrowAnException_WhenQueuesCollectionIsEmpty()
        {
            UseConnection(connection =>
            {
                var queue = CreateJobQueue(connection);

                var exception = Assert.Throws<ArgumentException>(() =>
                    queue.FetchNextJob(new string[0], CreateTimingOutCancellationToken()));

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

                Assert.Throws<OperationCanceledException>(() =>
                    queue.FetchNextJob(DefaultQueues, cts.Token));
            });
        }

        [Fact, CleanDatabase]
        public void Dequeue_ShouldWaitIndefinitely_WhenThereAreNoJobs()
        {
            UseConnection(connection =>
            {
                var cts = new CancellationTokenSource(200);
                var queue = CreateJobQueue(connection);

                Assert.ThrowsAny<OperationCanceledException>(() =>
                    queue.FetchNextJob(DefaultQueues, cts.Token));
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
                    JobId = ObjectId.GenerateNewId(),
                    Queue = "default"
                };

                connection.JobGraph.InsertOne(jobQueue);
                var token = CreateTimingOutCancellationToken();
                var queue = CreateJobQueue(connection);
                _jobQueueSemaphoreMock.Setup(m => m.WaitNonBlock("default")).Returns(true);
                
                // Act
                MongoFetchedJob payload = (MongoFetchedJob)queue.FetchNextJob(DefaultQueues, token);

                // Assert
                Assert.Equal(jobQueue.JobId.ToString(), payload.JobId);
                Assert.Equal("default", payload.Queue);
                
                _jobQueueSemaphoreMock.Verify(m => m.WaitNonBlock("default"), Times.Once);
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
                    CreatedAt = DateTime.UtcNow
                };
                connection.JobGraph.InsertOne(job);

                var jobQueue = new JobQueueDto
                {
                    JobId = job.Id,
                    Queue = "default"
                };
                connection.JobGraph.InsertOne(jobQueue);

                var queue = CreateJobQueue(connection);
                _jobQueueSemaphoreMock.Setup(m => m.WaitNonBlock("default")).Returns(true);
                // Act
                var payload = queue.FetchNextJob(DefaultQueues, CreateTimingOutCancellationToken());

                // Assert
                Assert.NotNull(payload);

                var fetchedAt = connection.JobGraph.OfType<JobQueueDto>()
                    .Find(Builders<JobQueueDto>.Filter.Eq(_ => _.JobId, ObjectId.Parse(payload.JobId)))
                    .FirstOrDefault()
                    .FetchedAt;

                Assert.NotNull(fetchedAt);
                Assert.True(fetchedAt > DateTime.UtcNow.AddMinutes(-1));
                _jobQueueSemaphoreMock.Verify(m => m.WaitNonBlock("default"), Times.Once);
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
                    CreatedAt = DateTime.UtcNow
                };
                connection.JobGraph.InsertOne(job);

                var jobQueue = new JobQueueDto
                {
                    JobId = job.Id,
                    Queue = "default",
                    FetchedAt = DateTime.UtcNow.AddDays(-1)
                };
                connection.JobGraph.InsertOne(jobQueue);
                _jobQueueSemaphoreMock.Setup(m => m.WaitNonBlock("default")).Returns(true);
                var queue = CreateJobQueue(connection);

                // Act
                var payload = queue.FetchNextJob(DefaultQueues, CreateTimingOutCancellationToken());

                // Assert
                Assert.NotEmpty(payload.JobId);
                _jobQueueSemaphoreMock.Verify(m => m.WaitNonBlock("default"), Times.Once);
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
                    CreatedAt = DateTime.UtcNow
                };
                connection.JobGraph.InsertOne(job1);

                var job2 = new JobDto
                {
                    InvocationData = "",
                    Arguments = "",
                    CreatedAt = DateTime.UtcNow
                };
                connection.JobGraph.InsertOne(job2);

                connection.JobGraph.InsertOne(new JobQueueDto
                {
                    JobId = job1.Id,
                    Queue = "default"
                });

                connection.JobGraph.InsertOne(new JobQueueDto
                {
                    JobId = job2.Id,
                    Queue = "default"
                });

                var queue = CreateJobQueue(connection);
                
                _jobQueueSemaphoreMock.Setup(m => m.WaitNonBlock("default")).Returns(true);

                // Act
                var payload = queue.FetchNextJob(DefaultQueues, CreateTimingOutCancellationToken());

                // Assert
                var otherJobFetchedAt = connection
                    .JobGraph.OfType<JobQueueDto>().Find(Builders<JobQueueDto>.Filter.Ne(_ => _.JobId, ObjectId.Parse(payload.JobId)))
                    .FirstOrDefault()
                    .FetchedAt;

                Assert.Null(otherJobFetchedAt);
                _jobQueueSemaphoreMock.Verify(m => m.WaitNonBlock("default"), Times.Once);
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
                    CreatedAt = DateTime.UtcNow
                };
                connection.JobGraph.InsertOne(job1);

                connection.JobGraph.InsertOne(new JobQueueDto
                {
                    JobId = job1.Id,
                    Queue = "critical"
                });


                var queue = CreateJobQueue(connection);
                
                Assert.ThrowsAny<OperationCanceledException>(() => queue.FetchNextJob(DefaultQueues, CreateTimingOutCancellationToken()));
            });
        }

        [Fact, CleanDatabase]
        public void Dequeue_ShouldFetchJobs_FromMultipleQueuesBasedOnQueuePriority()
        {
            UseConnection(connection =>
            {
                var criticalJob = new JobDto
                {
                    InvocationData = "",
                    Arguments = "",
                    CreatedAt = DateTime.UtcNow
                };
                connection.JobGraph.InsertOne(criticalJob);

                var defaultJob = new JobDto
                {
                    InvocationData = "",
                    Arguments = "",
                    CreatedAt = DateTime.UtcNow
                };
                connection.JobGraph.InsertOne(defaultJob);

                connection.JobGraph.InsertOne(new JobQueueDto
                {
                    JobId = defaultJob.Id,
                    Queue = "default"
                });

                connection.JobGraph.InsertOne(new JobQueueDto
                {
                    JobId = criticalJob.Id,
                    Queue = "critical"
                });

                var queue = CreateJobQueue(connection);
                _jobQueueSemaphoreMock.Setup(m => m.WaitNonBlock("critical")).Returns(true);
                _jobQueueSemaphoreMock.Setup(m => m.WaitNonBlock("default")).Returns(true);
                
                var critical = (MongoFetchedJob)queue.FetchNextJob(
                    new[] { "critical", "default" },
                    CreateTimingOutCancellationToken());

                Assert.NotNull(critical.JobId);
                Assert.Equal("critical", critical.Queue);

                var @default = (MongoFetchedJob)queue.FetchNextJob(
                    new[] { "critical", "default" },
                    CreateTimingOutCancellationToken());

                Assert.NotNull(@default.JobId);
                Assert.Equal("default", @default.Queue);
                
                _jobQueueSemaphoreMock.Verify(m => m.WaitNonBlock("critical"), Times.Once);
                _jobQueueSemaphoreMock.Verify(m => m.WaitNonBlock("default"), Times.Once);
            });
        }

        private static CancellationToken CreateTimingOutCancellationToken()
        {
            var source = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            return source.Token;
        }

        private MongoJobFetcher CreateJobQueue(HangfireDbContext connection)
        {
            return new MongoJobFetcher(connection, new MongoStorageOptions(), _jobQueueSemaphoreMock.Object);
        }

        private static void UseConnection(Action<HangfireDbContext> action)
        {
            using (var connection = ConnectionUtils.CreateDbContext())
            {
                action(connection);
            }
        }
    }
#pragma warning restore 1591
}