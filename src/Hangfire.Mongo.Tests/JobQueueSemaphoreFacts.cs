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
        private readonly string[] _testQueues = {"test"};
        
        public JobQueueSemaphoreFacts()
        {
            _semaphore = new JobQueueSemaphore();
        }

        [Fact]
        private async Task WaitAny_TimesOut_NullReturned()
        {
            // ARRANGE
            var waitTask = Task.Run(async () =>
            {
                await Task.Yield();
                _semaphore.WaitAny(_testQueues, CancellationToken.None, TimeSpan.FromMilliseconds(500), out var q, out var timedOut);
                return q;
            });
            
            // ACT
            var result = await waitTask;
            
            // ASSERT
            Assert.Null(result);
        }
        
        [Fact]
        private async Task WaitAny_Cancelled_NullReturned()
        {
            // ARRANGE
            var cts = new CancellationTokenSource(200);
            var waitTask = Task.Run(async () =>
            {
                await Task.Yield();
                _semaphore.WaitAny(_testQueues, cts.Token, TimeSpan.FromMilliseconds(5000), out var q, out var timedOut);
                return q;
            }, cts.Token);
            
            // ACT
            var result = await waitTask;
            
            // ASSERT
            Assert.Null(result);
        }
        
        [Fact]
        private async Task WaitAny_Released_QueueNameReturned()
        {
            // ARRANGE
            var waitTask = Task.Run(async () =>
            {
                await Task.Yield();
                _semaphore.WaitAny(_testQueues, CancellationToken.None, TimeSpan.FromMilliseconds(5000), out var q, out var timedOut);
                return q;
            });
            
            // ACT
            Thread.Sleep(100);
            _semaphore.Release(_testQueues.First());
            var result = await waitTask;
            
            // ASSERT
            Assert.Equal(_testQueues.First(), result);
        }
        
        [Fact]
        private void WaitAny_AlreadyReleased_QueueNameReturnedImmediately()
        {
            // ARRANGE
            _semaphore.Release(_testQueues.First());
            
            // ACT
            _semaphore.WaitAny(_testQueues, CancellationToken.None, TimeSpan.FromMilliseconds(100), out var result, out var timedOut);
            
            // ASSERT
            Assert.Equal(_testQueues.First(), result);
        }
        
        [Fact]
        private void WaitAny_ReleasedMultipleTimes_CanBeWaitedMultipleTimes()
        {
            // ARRANGE
            _semaphore.Release(_testQueues.First());
            _semaphore.Release(_testQueues.First());
            var results = new string[2];
            var timedOut = false;
            
            // ACT
            _semaphore.WaitAny(_testQueues, CancellationToken.None, TimeSpan.FromMilliseconds(100), out results[0], out timedOut);
            _semaphore.WaitAny(_testQueues, CancellationToken.None, TimeSpan.FromMilliseconds(100), out results[1], out timedOut);
            
            // ASSERT
            Assert.Equal(_testQueues.First(), results[0]);
            Assert.Equal(_testQueues.First(), results[1]);
        }
        
        [Fact]
        private void WaitAny_QueueNotPresent_TimesOut()
        {
            // ARRANGE
            var queues = new[] { "queue" };

            // ACT
            _semaphore.Release("another-queue");
            _semaphore.WaitAny(queues, CancellationToken.None, TimeSpan.FromMilliseconds(100), out var queue, out var timedOut);
            _semaphore.Release("queue");
            _semaphore.WaitAny(queues, CancellationToken.None, TimeSpan.FromMilliseconds(100), out var queue1, out var timedOut1);

            // ASSERT
            Assert.True(timedOut);
            Assert.True(string.IsNullOrEmpty(queue));
            Assert.False(timedOut1);
            Assert.Equal("queue", queue1);
        }

        public void Dispose()
        {
            _semaphore.Dispose();
        }
    }
}