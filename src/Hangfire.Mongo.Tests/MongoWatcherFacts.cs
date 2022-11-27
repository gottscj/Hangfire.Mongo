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
    public sealed class MongoWatcherFacts : IDisposable
    {
        private readonly HangfireDbContext _dbContext;

        private readonly Mock<IJobQueueSemaphore> _jobQueueSemaphoreMock;
        private readonly CancellationTokenSource _cts;
        public MongoWatcherFacts()
        {
            _dbContext = ConnectionUtils.CreateDbContext();
            _jobQueueSemaphoreMock = new Mock<IJobQueueSemaphore>(MockBehavior.Strict);
            var watcher = new MongoJobQueueWatcher(
                _dbContext,
                new MongoStorageOptions(),
                _jobQueueSemaphoreMock.Object);
            
            _cts = new CancellationTokenSource();
            
            Task.Run(async () =>
            {
                await Task.Yield();
                watcher.Execute(_cts.Token);
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
            _jobQueueSemaphoreMock.Setup(m => m.Release("test"))
                .Callback(() => signal.Release());
            
            // ACT
            _dbContext.JobGraph.InsertOne(new JobQueueDto
            {
                Queue = "test"
            }.Serialize());
            
            signal.Wait(100000);
            
            // ASSERT
            _jobQueueSemaphoreMock.Verify(m => m.Release("test"), Times.Once);
        }
    }
}