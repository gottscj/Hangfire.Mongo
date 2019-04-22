using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace Hangfire.Mongo.Tests
{
    public class DistributedLockMutexFacts
    {
        private const string TestResource = "test";
        
        [Fact]
        public void Release_OneWaiter_GetsAccess()
        {
            // ARRANGE
            var mutex = new DistributedLockMutex();
            var waitTask = CreateWaitTasks(1, mutex).First();
            
            // ACT
            mutex.Release(TestResource);
            var result = waitTask.Wait(2000);
            
            // ASSERT
            Assert.True(result);
        }

        [Fact]
        public void Release_MultipleWaiters_OneGetsAccess()
        {
            // ARRANGE
            var mutex = new DistributedLockMutex();
            var tasks = CreateWaitTasks(10, mutex);
            
            // ACT
            mutex.Release(TestResource);
            var result = Task.WhenAny(tasks).Unwrap().Wait(2000);
            
            // ASSERT
            Assert.True(result);
            Assert.Equal(1, tasks.Count(t => t.IsCompleted));
            Assert.Equal(9, tasks.Count(t => !t.IsCompleted));
        }

        private Task[] CreateWaitTasks(int count, IDistributedLockMutex mutex)
        {
            var tasks = new Task[count];
            for (int i = 0; i < count; i++)
            {
                tasks[i] = Task.Run(async () =>
                {
                    await Task.Yield();
                
                    mutex.Wait(TestResource, TimeSpan.FromSeconds(1));
                });
            }
            // wait a bit for tasks to get into waiting state.
            Thread.Sleep(200);
            return tasks;
        }
    }
}