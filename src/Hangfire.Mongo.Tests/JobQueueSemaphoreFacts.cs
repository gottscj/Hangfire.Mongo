using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace Hangfire.Mongo.Tests
{
    public sealed class JobQueueSemaphoreFacts : IDisposable
    {
        private readonly JobQueueSemaphore _semaphore;
        private readonly string[] TestQueues = new string[] {"test"};
        
        public JobQueueSemaphoreFacts()
        {
            _semaphore = new JobQueueSemaphore();
        }

        [Fact]
        private void WaitAny_TimesOut_NullReturned()
        {
            // ARRANGE
            var waitTask = Task.Run(async () =>
            {
                await Task.Yield();
                _semaphore.WaitAny(TestQueues, CancellationToken.None, TimeSpan.FromMilliseconds(500), out var q, out var timedOut);
                return q;
            });
            
            // ACT
            var result = waitTask.GetAwaiter().GetResult();
            
            // ASSERT
            Assert.Null(result);
        }
        
        [Fact]
        private void WaitAny_Cancelled_NullReturned()
        {
            // ARRANGE
            var cts = new CancellationTokenSource(200);
            var waitTask = Task.Run(async () =>
            {
                await Task.Yield();
                _semaphore.WaitAny(TestQueues, cts.Token, TimeSpan.FromMilliseconds(5000), out var q, out var timedOut);
                return q;
            });
            
            // ACT
            var result = waitTask.GetAwaiter().GetResult();
            
            // ASSERT
            Assert.Null(result);
        }
        
        [Fact]
        private void WaitAny_Released_QueueNameReturned()
        {
            // ARRANGE
            var waitTask = Task.Run(async () =>
            {
                await Task.Yield();
                _semaphore.WaitAny(TestQueues, CancellationToken.None, TimeSpan.FromMilliseconds(5000), out var q, out var timedOut);
                return q;
            });
            
            // ACT
            Thread.Sleep(100);
            _semaphore.Release(TestQueues.First());
            var result = waitTask.GetAwaiter().GetResult();
            
            // ASSERT
            Assert.Equal(TestQueues.First(), result);
        }
        
        [Fact]
        private void WaitAny_AlreadyReleased_QueueNameReturnedImmediately()
        {
            // ARRANGE
            _semaphore.Release(TestQueues.First());
            
            // ACT
            _semaphore.WaitAny(TestQueues, CancellationToken.None, TimeSpan.FromMilliseconds(100), out var result, out var timedOut);
            
            // ASSERT
            Assert.Equal(TestQueues.First(), result);
        }
        
        [Fact]
        private void WaitAny_ReleasedMultipleTimes_CanBeWaitedMultipleTimes()
        {
            // ARRANGE
            _semaphore.Release(TestQueues.First());
            _semaphore.Release(TestQueues.First());
            var results = new string[2];
            var timedOut = false;
            // ACT
            _semaphore.WaitAny(TestQueues, CancellationToken.None, TimeSpan.FromMilliseconds(100), out results[0], out timedOut);
            _semaphore.WaitAny(TestQueues, CancellationToken.None, TimeSpan.FromMilliseconds(100), out results[1], out timedOut);
            
            // ASSERT
            Assert.Equal(TestQueues.First(), results[0]);
            Assert.Equal(TestQueues.First(), results[1]);
        }

        public void Dispose()
        {
            _semaphore.Dispose();
        }
    }
}