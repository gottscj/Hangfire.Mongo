using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace Hangfire.Mongo.Tests
{
    public class DistributedLockMutexFacts
    {
        private readonly IDistributedLockMutex _mutex;
        private const string TestResource = "test";
        
        public DistributedLockMutexFacts()
        {
            _mutex = new DistributedLockMutex();
        }

        [Fact]
        public void Release_OneWaiter_GetsAccess()
        {
            // ARRANGE
            var waitTask = CreateWaitTasks(1).First();
            
            // ACT
            _mutex.Release(TestResource);
            var result = waitTask.Wait(2000);
            
            // ASSERT
            Assert.True(result);
        }

        [Fact]
        public void Release_MultipleWaiters_OneGetsAccess()
        {
            // ARRANGE
            var tasks = CreateWaitTasks(10);
            
            // ACT
            _mutex.Release(TestResource);
            var result = Task.WhenAny(tasks).Unwrap().Wait(2000);
            
            // ASSERT
            Assert.True(result);
            Assert.Equal(1, tasks.Count(t => t.IsCompleted));
            Assert.Equal(9, tasks.Count(t => !t.IsCompleted));
        }

        private Task[] CreateWaitTasks(int count)
        {
            var tasks = new Task[count];
            for (int i = 0; i < count; i++)
            {
                tasks[i] = Task.Run(async () =>
                {
                    await Task.Yield();
                
                    _mutex.Wait(TestResource, TimeSpan.FromSeconds(1));
                });
            }
            // wait a bit for tasks to get into waiting state.
            Thread.Sleep(100);
            return tasks;
        }
    }
}