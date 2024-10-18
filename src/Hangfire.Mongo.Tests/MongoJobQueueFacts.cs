using System;
using System.Threading;
using Hangfire.Mongo.Database;
using Hangfire.Mongo.Dto;
using Hangfire.Mongo.Tests.Utils;
using MongoDB.Bson;
using MongoDB.Driver;
using NSubstitute;
using Xunit;

namespace Hangfire.Mongo.Tests
{
#pragma warning disable 1591
    [Collection("Database")]
    public class MongoJobQueueFacts
    {
        private static readonly string[] DefaultQueues = { "default" };

        private readonly IJobQueueSemaphore _jobQueueSemaphoreMock;
        private readonly HangfireDbContext _hangfireDbContext;

        public MongoJobQueueFacts(MongoIntegrationTestFixture fixture)
        {
            _jobQueueSemaphoreMock = Substitute.For<IJobQueueSemaphore>();
            _jobQueueSemaphoreMock.WaitAny(DefaultQueues, default, default, out _, out _)
                .Returns(true);
            fixture.CleanDatabase();
            _hangfireDbContext = fixture.CreateDbContext();
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenDbContextIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(() =>
                new MongoJobFetcher(null, new MongoStorageOptions(), _jobQueueSemaphoreMock));

            Assert.Equal("dbContext", exception.ParamName);
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenOptionsValueIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(() =>
                new MongoJobFetcher(_hangfireDbContext, null, _jobQueueSemaphoreMock));

            Assert.Equal("storageOptions", exception.ParamName);
        }

        [Fact]
        public void Dequeue_ShouldThrowAnException_WhenQueuesCollectionIsNull()
        {
            var queue =new MongoJobFetcher(_hangfireDbContext, new MongoStorageOptions(), _jobQueueSemaphoreMock);

            var exception = Assert.Throws<ArgumentNullException>(() =>
                queue.FetchNextJob(null, CreateTimingOutCancellationToken()));

            Assert.Equal("queues", exception.ParamName);
        }

        [Fact]
        public void Dequeue_ShouldThrowAnException_WhenQueuesCollectionIsEmpty()
        {
            var queue =new MongoJobFetcher(_hangfireDbContext, new MongoStorageOptions(), _jobQueueSemaphoreMock);

            var exception = Assert.Throws<ArgumentException>(() =>
                queue.FetchNextJob(new string[0], CreateTimingOutCancellationToken()));

            Assert.Equal("queues", exception.ParamName);
        }

        [Fact]
        public void Dequeue_ThrowsOperationCanceled_WhenCancellationTokenIsSetAtTheBeginning()
        {
            var cts = new CancellationTokenSource();
            cts.Cancel();
            var queue =new MongoJobFetcher(_hangfireDbContext, new MongoStorageOptions(), _jobQueueSemaphoreMock);

            Assert.Throws<OperationCanceledException>(() =>
                queue.FetchNextJob(DefaultQueues, cts.Token));
        }

        [Fact]
        public void Dequeue_ShouldWaitIndefinitely_WhenThereAreNoJobs()
        {
            var cts = new CancellationTokenSource(200);
            var queue =new MongoJobFetcher(_hangfireDbContext, new MongoStorageOptions(), _jobQueueSemaphoreMock);

            Assert.ThrowsAny<OperationCanceledException>(() =>
                queue.FetchNextJob(DefaultQueues, cts.Token));
        }

        [Fact]
        public void Dequeue_ShouldFetchAJob_FromTheSpecifiedQueue()
        {
            // Arrange
            var job = new JobDto
            {
                Queue = "default"
            };

            _hangfireDbContext.JobGraph.InsertOne(job.Serialize());
            var token = CreateTimingOutCancellationToken();
            var queue =new MongoJobFetcher(_hangfireDbContext, new MongoStorageOptions(), _jobQueueSemaphoreMock);
            _jobQueueSemaphoreMock.WaitNonBlock("default").Returns(true);

            // Act
            MongoFetchedJob payload = (MongoFetchedJob)queue.FetchNextJob(DefaultQueues, token);

            // Assert
            Assert.Equal(job.Id.ToString(), payload.JobId);
            Assert.Equal("default", payload.Queue);

            _jobQueueSemaphoreMock.Received(1).WaitNonBlock("default");
        }

        [Fact]
        public void Dequeue_ShouldLeaveJobInTheQueue_ButSetItsFetchedAtValue()
        {
            // Arrange
            var job = new JobDto
            {
                InvocationData = "",
                Arguments = "",
                CreatedAt = DateTime.UtcNow,
                Queue = "default"
            };
            _hangfireDbContext.JobGraph.InsertOne(job.Serialize());


            var queue =new MongoJobFetcher(_hangfireDbContext, new MongoStorageOptions(), _jobQueueSemaphoreMock);
            _jobQueueSemaphoreMock.WaitNonBlock("default").Returns(true);
            // Act
            var payload = queue.FetchNextJob(DefaultQueues, CreateTimingOutCancellationToken());

            // Assert
            Assert.NotNull(payload);
            var filter = new BsonDocument
            {
                ["_t"] = nameof(JobDto),
                ["_id"] = ObjectId.Parse(payload.JobId)
            };
            var document = _hangfireDbContext.JobGraph.Find(filter).FirstOrDefault();
            var fetchedAt = new JobDto(document).FetchedAt;

            Assert.NotNull(document);
            Assert.True(fetchedAt > DateTime.UtcNow.AddMinutes(-1));
            _jobQueueSemaphoreMock.Received(1).WaitNonBlock("default");
        }

        [Fact]
        public void Dequeue_ShouldFetchATimedOutJobs_FromTheSpecifiedQueue()
        {
            // Arrange
            var job = new JobDto
            {
                InvocationData = "",
                Arguments = "",
                CreatedAt = DateTime.UtcNow,
                Queue = "default",
                FetchedAt = DateTime.UtcNow.AddDays(-1)
            };
            _hangfireDbContext.JobGraph.InsertOne(job.Serialize());

            var options = new MongoStorageOptions
            {
                SlidingInvisibilityTimeout = TimeSpan.FromMinutes(30)
            };

            _jobQueueSemaphoreMock.WaitNonBlock("default").Returns(true);
            var queue =new MongoJobFetcher(_hangfireDbContext, options, _jobQueueSemaphoreMock);

            // Act
            var payload = queue.FetchNextJob(DefaultQueues, CreateTimingOutCancellationToken());

            // Assert
            Assert.NotEmpty(payload.JobId);
            _jobQueueSemaphoreMock.Received(1).WaitNonBlock("default");
        }

        [Fact]
        public void Dequeue_NoInvisibilityTimeout_WaitsForever()
        {
            // Arrange
            var job = new JobDto
            {
                InvocationData = "",
                Arguments = "",
                CreatedAt = DateTime.UtcNow,
                Queue = "default",
                FetchedAt = DateTime.UtcNow.AddDays(-1)
            };
            _hangfireDbContext.JobGraph.InsertOne(job.Serialize());

            var options = new MongoStorageOptions
            {
                SlidingInvisibilityTimeout = null
            };

            _jobQueueSemaphoreMock.WaitNonBlock("default").Returns(true);
            var queue =new MongoJobFetcher(_hangfireDbContext, options, _jobQueueSemaphoreMock);

            // Act
            var exception =
                Assert.Throws<OperationCanceledException>(() =>
                    queue.FetchNextJob(DefaultQueues,
                        CreateTimingOutCancellationToken(TimeSpan.FromMilliseconds(200))));

            // Assert
            Assert.NotNull(exception);
            _jobQueueSemaphoreMock.DidNotReceive().WaitNonBlock("default");
        }

        [Fact]
        public void Dequeue_ShouldSetFetchedAt_OnlyForTheFetchedJob()
        {
            // Arrange
            var job1 = new JobDto
            {
                InvocationData = "",
                Arguments = "",
                CreatedAt = DateTime.UtcNow,
                Queue = "default"
            };
            _hangfireDbContext.JobGraph.InsertOne(job1.Serialize());

            var job2 = new JobDto
            {
                InvocationData = "",
                Arguments = "",
                CreatedAt = DateTime.UtcNow,
                Queue = "default"
            };
            _hangfireDbContext.JobGraph.InsertOne(job2.Serialize());

            var queue =new MongoJobFetcher(_hangfireDbContext, new MongoStorageOptions(), _jobQueueSemaphoreMock);

            _jobQueueSemaphoreMock.WaitNonBlock("default").Returns(true);

            // Act
            var payload = queue.FetchNextJob(DefaultQueues, CreateTimingOutCancellationToken());

            // Assert
            var filter = new BsonDocument
            {
                ["_t"] = nameof(JobDto),
                ["_id"] = new BsonDocument("$ne", ObjectId.Parse(payload.JobId))
            };
            var document = _hangfireDbContext.JobGraph.Find(filter).FirstOrDefault();
            Assert.NotNull(document);

            var otherJob = new JobDto(document);
            var otherJobFetchedAt = otherJob.FetchedAt;

            Assert.Null(otherJobFetchedAt);
            _jobQueueSemaphoreMock.Received(1).WaitNonBlock("default");
        }

        [Fact]
        public void Dequeue_ShouldFetchJobs_OnlyFromSpecifiedQueues()
        {
            var job1 = new JobDto
            {
                InvocationData = "",
                Arguments = "",
                CreatedAt = DateTime.UtcNow,
                Queue = "critical"
            };
            _hangfireDbContext.JobGraph.InsertOne(job1.Serialize());


            var queue =new MongoJobFetcher(_hangfireDbContext, new MongoStorageOptions(), _jobQueueSemaphoreMock);

            Assert.ThrowsAny<OperationCanceledException>(() => queue.FetchNextJob(DefaultQueues, CreateTimingOutCancellationToken()));
        }

        [Fact]
        public void Dequeue_ShouldFetchJobs_FromMultipleQueuesBasedOnQueuePriority()
        {
            var criticalJob = new JobDto
            {
                InvocationData = "",
                Arguments = "",
                CreatedAt = DateTime.UtcNow,
                Queue = "critical"
            };
            _hangfireDbContext.JobGraph.InsertOne(criticalJob.Serialize());

            var defaultJob = new JobDto
            {
                InvocationData = "",
                Arguments = "",
                CreatedAt = DateTime.UtcNow,
                Queue = "default"
            };
            _hangfireDbContext.JobGraph.InsertOne(defaultJob.Serialize());


            var queue =new MongoJobFetcher(_hangfireDbContext, new MongoStorageOptions(), _jobQueueSemaphoreMock);
            _jobQueueSemaphoreMock.WaitNonBlock("critical").Returns(true);
            _jobQueueSemaphoreMock.WaitNonBlock("default").Returns(true);

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

            _jobQueueSemaphoreMock.Received(1).WaitNonBlock("critical");
            _jobQueueSemaphoreMock.Received(1).WaitNonBlock("default");
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