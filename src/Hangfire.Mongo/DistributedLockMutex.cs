using System;
using System.Collections.Concurrent;
using System.Threading;
using Hangfire.Logging;

namespace Hangfire.Mongo
{
    internal interface IDistributedLockMutex
    {
        DateTime Wait(string resource, TimeSpan timeout);
        void Release(string resource);
    }

    internal class DistributedLockMutex : IDistributedLockMutex
    {
        private static readonly ILog Logger = LogProvider.For<DistributedLockMutex>();
        public static readonly IDistributedLockMutex Instance = new DistributedLockMutex();
        
        private readonly ConcurrentDictionary<string, SemaphoreSlim> _pool = new ConcurrentDictionary<string, SemaphoreSlim>();

        public DateTime Wait(string resource, TimeSpan timeout)
        {
            if (Logger.IsDebugEnabled())
            {
                Logger.Debug($"{resource} - Waiting {timeout}ms");    
            }
            
            var semaphore = _pool.GetOrAdd(resource, new SemaphoreSlim(0, 1));
            var signaled = semaphore.Wait(timeout);
            
            if (Logger.IsDebugEnabled())
            {
                if (signaled)
                {
                    Logger.Debug($"{resource} - received signal... retrying");     
                }
                Logger.Debug($"{resource} - Wait timed out... retrying");    
            }
            return DateTime.UtcNow;
        }

        public void Release(string resource)
        {
            if(!_pool.TryGetValue(resource, out var semaphore))
            {
                return;
            }
            
            semaphore.Release(1);    
            if(Logger.IsDebugEnabled())
            {
                Logger.Debug(
                $"Released Resource: '{resource}', for release " +
                $" Thread[{Thread.CurrentThread.ManagedThreadId}]");                
            }
        }
    }
}