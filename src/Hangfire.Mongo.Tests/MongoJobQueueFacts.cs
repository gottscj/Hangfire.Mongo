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
        private readonly HangfireDbContext _hangfireDbContext;
        public MongoJobQueueFacts()
        {
            _jobQueueSemaphoreMock = new Mock<IJobQueueSemaphore>(MockBehavior.Strict);
            var queue = "default";
            var timedOut = false;
            _jobQueueSemaphoreMock.Setup(s =>
                    s.WaitAny(DefaultQueues, It.IsAny<CancellationToken>(), It.IsAny<TimeSpan>(), out queue, out timedOut))
                .Returns(true);
            _hangfireDbContext = ConnectionUtils.CreateDbContext();
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
            var exception = Assert.Throws<ArgumentNullException>(() =>
                new MongoJobFetcher(_hangfireDbContext, null, _jobQueueSemaphoreMock.Object));

            Assert.Equal("storageOptions", exception.ParamName);
        }

        [Fact, CleanDatabase]
        public void Dequeue_ShouldThrowAnException_WhenQueuesCollectionIsNull()
        {
            var queue =new MongoJobFetcher(_hangfireDbContext, new MongoStorageOptions(), _jobQueueSemaphoreMock.Object);

            var exception = Assert.Throws<ArgumentNullException>(() =>
                queue.FetchNextJob(null, CreateTimingOutCancellationToken()));

            Assert.Equal("queues", exception.ParamName);
        }

        [Fact, CleanDatabase]
        public void Dequeue_ShouldThrowAnException_WhenQueuesCollectionIsEmpty()
        {
            var queue =new MongoJobFetcher(_hangfireDbContext, new MongoStorageOptions(), _jobQueueSemaphoreMock.Object);

            var exception = Assert.Throws<ArgumentException>(() =>
                queue.FetchNextJob(new string[0], CreateTimingOutCancellationToken()));

            Assert.Equal("queues", exception.ParamName);
        }

        [Fact]
        public void Dequeue_ThrowsOperationCanceled_WhenCancellationTokenIsSetAtTheBeginning()
        {
            var cts = new CancellationTokenSource();
            cts.Cancel();
            var queue =new MongoJobFetcher(_hangfireDbContext, new MongoStorageOptions(), _jobQueueSemaphoreMock.Object);

            Assert.Throws<OperationCanceledException>(() =>
                queue.FetchNextJob(DefaultQueues, cts.Token));
        }

        [Fact, CleanDatabase]
        public void Dequeue_ShouldWaitIndefinitely_WhenThereAreNoJobs()
        {
            var cts = new CancellationTokenSource(200);
            var queue =new MongoJobFetcher(_hangfireDbContext, new MongoStorageOptions(), _jobQueueSemaphoreMock.Object);

            Assert.ThrowsAny<OperationCanceledException>(() =>
                queue.FetchNextJob(DefaultQueues, cts.Token));
        }

        [Fact, CleanDatabase]
        public void Dequeue_ShouldFetchAJob_FromTheSpecifiedQueue()
        {
            // Arrange
            var jobQueue = new JobQueueDto
            {
                JobId = ObjectId.GenerateNewId(),
                Queue = "default"
            };

            _hangfireDbContext.JobGraph.InsertOne(jobQueue);
            var token = CreateTimingOutCancellationToken();
            var queue =new MongoJobFetcher(_hangfireDbContext, new MongoStorageOptions(), _jobQueueSemaphoreMock.Object);
            _jobQueueSemaphoreMock.Setup(m => m.WaitNonBlock("default")).Returns(true);
                
            // Act
            MongoFetchedJob payload = (MongoFetchedJob)queue.FetchNextJob(DefaultQueues, token);

            // Assert
            Assert.Equal(jobQueue.JobId.ToString(), payload.JobId);
            Assert.Equal("default", payload.Queue);
                
            _jobQueueSemaphoreMock.Verify(m => m.WaitNonBlock("default"), Times.Once);
        }

        [Fact, CleanDatabase]
        public void Dequeue_ShouldLeaveJobInTheQueue_ButSetItsFetchedAtValue()
        {
            // Arrange
            var job = new JobDto
            {
                InvocationData = "",
                Arguments = "",
                CreatedAt = DateTime.UtcNow
            };
            _hangfireDbContext.JobGraph.InsertOne(job);

            var jobQueue = new JobQueueDto
            {
                JobId = job.Id,
                Queue = "default"
            };
            _hangfireDbContext.JobGraph.InsertOne(jobQueue);

            var queue =new MongoJobFetcher(_hangfireDbContext, new MongoStorageOptions(), _jobQueueSemaphoreMock.Object);
            _jobQueueSemaphoreMock.Setup(m => m.WaitNonBlock("default")).Returns(true);
            // Act
            var payload = queue.FetchNextJob(DefaultQueues, CreateTimingOutCancellationToken());

            // Assert
            Assert.NotNull(payload);

            var fetchedAt = _hangfireDbContext.JobGraph.OfType<JobQueueDto>()
                .Find(Builders<JobQueueDto>.Filter.Eq(_ => _.JobId, ObjectId.Parse(payload.JobId)))
                .FirstOrDefault()
                .FetchedAt;

            Assert.NotNull(fetchedAt);
            Assert.True(fetchedAt > DateTime.UtcNow.AddMinutes(-1));
            _jobQueueSemaphoreMock.Verify(m => m.WaitNonBlock("default"), Times.Once);
        }

        [Fact, CleanDatabase]
        public void Dequeue_ShouldFetchATimedOutJobs_FromTheSpecifiedQueue()
        {
            // Arrange
            var job = new JobDto
            {
                InvocationData = "",
                Arguments = "",
                CreatedAt = DateTime.UtcNow
            };
            _hangfireDbContext.JobGraph.InsertOne(job);

            var jobQueue = new JobQueueDto
            {
                JobId = job.Id,
                Queue = "default",
                FetchedAt = DateTime.UtcNow.AddDays(-1)
            };
            var options = new MongoStorageOptions
            {
                InvisibilityTimeout = TimeSpan.FromMinutes(30)
            };
            
            _hangfireDbContext.JobGraph.InsertOne(jobQueue);
            _jobQueueSemaphoreMock.Setup(m => m.WaitNonBlock("default")).Returns(true);
            var queue =new MongoJobFetcher(_hangfireDbContext, options, _jobQueueSemaphoreMock.Object);

            // Act
            var payload = queue.FetchNextJob(DefaultQueues, CreateTimingOutCancellationToken());

            // Assert
            Assert.NotEmpty(payload.JobId);
            _jobQueueSemaphoreMock.Verify(m => m.WaitNonBlock("default"), Times.Once);
        }
        
        [Fact, CleanDatabase]
        public void Dequeue_NoInvisibilityTimeout_WaitsForever()
        {
            // Arrange
            var job = new JobDto
            {
                InvocationData = "",
                Arguments = "",
                CreatedAt = DateTime.UtcNow
            };
            _hangfireDbContext.JobGraph.InsertOne(job);

            var jobQueue = new JobQueueDto
            {
                JobId = job.Id,
                Queue = "default",
                FetchedAt = DateTime.UtcNow.AddDays(-1)
            };
            var options = new MongoStorageOptions();
            
            _hangfireDbContext.JobGraph.InsertOne(jobQueue);
            _jobQueueSemaphoreMock.Setup(m => m.WaitNonBlock("default")).Returns(true);
            var queue =new MongoJobFetcher(_hangfireDbContext, options, _jobQueueSemaphoreMock.Object);

            // Act
            var exception =
                Assert.Throws<OperationCanceledException>(() =>
                    queue.FetchNextJob(DefaultQueues,
                        CreateTimingOutCancellationToken(TimeSpan.FromMilliseconds(200))));

            // Assert
            Assert.NotNull(exception);
            _jobQueueSemaphoreMock.Verify(m => m.WaitNonBlock("default"), Times.Never);
        }

        [Fact, CleanDatabase]
        public void Dequeue_ShouldSetFetchedAt_OnlyForTheFetchedJob()
        {
            // Arrange
            var job1 = new JobDto
            {
                InvocationData = "",
                Arguments = "",
                CreatedAt = DateTime.UtcNow
            };
            _hangfireDbContext.JobGraph.InsertOne(job1);

            var job2 = new JobDto
            {
                InvocationData = "",
                Arguments = "",
                CreatedAt = DateTime.UtcNow
            };
            _hangfireDbContext.JobGraph.InsertOne(job2);

            _hangfireDbContext.JobGraph.InsertOne(new JobQueueDto
            {
                JobId = job1.Id,
                Queue = "default"
            });

            _hangfireDbContext.JobGraph.InsertOne(new JobQueueDto
            {
                JobId = job2.Id,
                Queue = "default"
            });

            var queue =new MongoJobFetcher(_hangfireDbContext, new MongoStorageOptions(), _jobQueueSemaphoreMock.Object);
                
            _jobQueueSemaphoreMock.Setup(m => m.WaitNonBlock("default")).Returns(true);

            // Act
            var payload = queue.FetchNextJob(DefaultQueues, CreateTimingOutCancellationToken());

            // Assert
            var otherJobFetchedAt = _hangfireDbContext
                .JobGraph.OfType<JobQueueDto>().Find(Builders<JobQueueDto>.Filter.Ne(_ => _.JobId, ObjectId.Parse(payload.JobId)))
                .FirstOrDefault()
                .FetchedAt;

            Assert.Null(otherJobFetchedAt);
            _jobQueueSemaphoreMock.Verify(m => m.WaitNonBlock("default"), Times.Once);
        }

        [Fact, CleanDatabase]
        public void Dequeue_ShouldFetchJobs_OnlyFromSpecifiedQueues()
        {
            var job1 = new JobDto
            {
                InvocationData = "",
                Arguments = "",
                CreatedAt = DateTime.UtcNow
            };
            _hangfireDbContext.JobGraph.InsertOne(job1);

            _hangfireDbContext.JobGraph.InsertOne(new JobQueueDto
            {
                JobId = job1.Id,
                Queue = "critical"
            });


            var queue =new MongoJobFetcher(_hangfireDbContext, new MongoStorageOptions(), _jobQueueSemaphoreMock.Object);
                
            Assert.ThrowsAny<OperationCanceledException>(() => queue.FetchNextJob(DefaultQueues, CreateTimingOutCancellationToken()));
        }

        [Fact, CleanDatabase]
        public void Dequeue_ShouldFetchJobs_FromMultipleQueuesBasedOnQueuePriority()
        {
            var criticalJob = new JobDto
            {
                InvocationData = "",
                Arguments = "",
                CreatedAt = DateTime.UtcNow
            };
            _hangfireDbContext.JobGraph.InsertOne(criticalJob);

            var defaultJob = new JobDto
            {
                InvocationData = "",
                Arguments = "",
                CreatedAt = DateTime.UtcNow
            };
            _hangfireDbContext.JobGraph.InsertOne(defaultJob);

            _hangfireDbContext.JobGraph.InsertOne(new JobQueueDto
            {
                JobId = defaultJob.Id,
                Queue = "default"
            });

            _hangfireDbContext.JobGraph.InsertOne(new JobQueueDto
            {
                JobId = criticalJob.Id,
                Queue = "critical"
            });

            var queue =new MongoJobFetcher(_hangfireDbContext, new MongoStorageOptions(), _jobQueueSemaphoreMock.Object);
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
        }

        private static CancellationToken CreateTimingOutCancellationToken(TimeSpan timeSpan = default(TimeSpan))
        {
            timeSpan = timeSpan == default(TimeSpan) ? TimeSpan.FromSeconds(10) : timeSpan;
            
            var source = new CancellationTokenSource(timeSpan);
            return source.Token;
        }
    }
#pragma warning restore 1591
}