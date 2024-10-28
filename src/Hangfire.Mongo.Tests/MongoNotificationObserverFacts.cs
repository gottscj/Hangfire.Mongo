using System;
using System.Threading;
using System.Threading.Tasks;
using Hangfire.Mongo.Database;
using Hangfire.Mongo.Dto;
using Hangfire.Mongo.Migration.Steps.Version17;
using Hangfire.Mongo.Tests.Utils;
using NSubstitute;
using Xunit;

namespace Hangfire.Mongo.Tests
{
    [Collection("Database")]
    public sealed class MongoNotificationObserverFacts : IDisposable
    {
        private readonly HangfireDbContext _dbContext;

        private readonly IJobQueueSemaphore _jobQueueSemaphoreMock;
        private readonly CancellationTokenSource _cts;

        public MongoNotificationObserverFacts(MongoIntegrationTestFixture fixture)
        {
            _dbContext = fixture.CreateDbContext();
            _jobQueueSemaphoreMock = Substitute.For<IJobQueueSemaphore>();
            var mongoNotificationObserver = new MongoNotificationObserver(
                _dbContext,
                new MongoStorageOptions(),
                _jobQueueSemaphoreMock);

            _dbContext.Database.DropCollection(_dbContext.Notifications.CollectionNamespace.CollectionName);
            var migration = new AddNotificationsCollection();
            migration.Execute(_dbContext.Database, new MongoStorageOptions(), null);
            _cts = new CancellationTokenSource();

            Task.Run(async () =>
            {
                await Task.Yield();
                mongoNotificationObserver.Execute(_cts.Token);
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

            // ACT
            _dbContext.Notifications.InsertOne(NotificationDto.JobEnqueued("test").Serialize());
            signal.Wait(1000);

            // ASSERT
            _jobQueueSemaphoreMock.Received(1).Release("test");
        }
    }
}