using System;
using System.Threading;
using System.Threading.Tasks;
using Hangfire.Mongo.Database;
using Hangfire.Mongo.Dto;
using Hangfire.Mongo.Tests.Utils;
using Moq;
using Xunit;

namespace Hangfire.Mongo.Tests
{
    public sealed class MongoNotificationObserverErrorFacts : IDisposable
    {
        private readonly HangfireDbContext _dbContext;

        private readonly Mock<IJobQueueSemaphore> _jobQueueSemaphoreMock;
        private readonly Mock<IDistributedLockMutex> _distributedLockMutexMock;
        private readonly CancellationTokenSource _cts;
        public MongoNotificationObserverErrorFacts()
        {
            _dbContext = ConnectionUtils.CreateDbContext();
            _jobQueueSemaphoreMock = new Mock<IJobQueueSemaphore>(MockBehavior.Strict);
            _distributedLockMutexMock = new Mock<IDistributedLockMutex>(MockBehavior.Strict);
            var mongoNotificationObserver = new MongoNotificationObserver(_dbContext, _jobQueueSemaphoreMock.Object,
                _distributedLockMutexMock.Object);
            
            _dbContext.Database.DropCollection(_dbContext.Notifications.CollectionNamespace.CollectionName);
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
        public void Execute_CollectionNotCapped_Stops()
        {
            // ARRANGE
            var signal = new SemaphoreSlim(0,1);
            _jobQueueSemaphoreMock.Setup(m => m.Release("test"))
                .Callback(() => signal.Release());
            
            // ACT
            _dbContext.Notifications.InsertOne(NotificationDto.JobEnqueued("test"));
            var signalled = signal.Wait(1000);
            
            // ASSERT
            Assert.False(signalled);
            _jobQueueSemaphoreMock.Verify(m => m.Release("test"), Times.Never);
            _distributedLockMutexMock.Verify(m => m.Release(It.IsAny<string>()), Times.Never);
        }
    }
}