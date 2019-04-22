using System;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace Hangfire.Mongo.Tests
{
    public class DistributedLockMutexFacts
    {
        private readonly TimeSpan _timeout = TimeSpan.FromSeconds(4);
        [Fact]
        public void Release_OneWaiter_GetsAccess()
        {
            // ARRANGE
            var resource = nameof(Release_OneWaiter_GetsAccess);
            var mutex = new DistributedLockMutex();
            var waitTask = CreateWaitTasks(resource, 1, mutex).First();
            
            // ACT
            mutex.Release(resource);
            var result = waitTask.Wait(_timeout);
            
            // ASSERT
            Assert.True(result);
        }

        [Fact]
        public void Release_MultipleWaiters_OneGetsAccess()
        {
            // ARRANGE
            var resource = nameof(Release_MultipleWaiters_OneGetsAccess);
            var mutex = new DistributedLockMutex();
            var tasks = CreateWaitTasks(resource, 10, mutex);
            
            // ACT
            mutex.Release(resource);
            var result = Task.WhenAny(tasks).Unwrap().Wait(_timeout);
            
            // ASSERT
            Assert.True(result);
            Assert.Equal(1, tasks.Count(t => t.IsCompleted));
            Assert.Equal(9, tasks.Count(t => !t.IsCompleted));
        }

        private Task[] CreateWaitTasks(string resource, int count, IDistributedLockMutex mutex)
        {
            var tasks = new Task[count];
            for (int i = 0; i < count; i++)
            {
                tasks[i] = Task.Factory.StartNew(() =>
                {
                    mutex.Wait(resource, TimeSpan.FromSeconds(2));
                }, TaskCreationOptions.LongRunning);
            }

            do
            {
                // wait until all tasks are running
                Thread.Sleep(100);
            } while (tasks.Any(t => t.Status != TaskStatus.Running));
            
            return tasks;
        }
    }
}