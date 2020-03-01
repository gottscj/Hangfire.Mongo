using System;
using System.Collections.Generic;
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
        
        private readonly Dictionary<string, SemaphoreSlim> _pool = new Dictionary<string, SemaphoreSlim>();
        private readonly object _synRoot = new object();
        
        public DateTime Wait(string resource, TimeSpan timeout)
        {
            if (Logger.IsTraceEnabled())
            {
                Logger.Trace($"{resource} - Waiting {timeout}ms");    
            }

            SemaphoreSlim semaphore;
            lock (_synRoot)
            {
                if (!_pool.TryGetValue(resource, out semaphore))
                {
                    semaphore = new SemaphoreSlim(0,1);
                    _pool[resource] = semaphore;
                }
            }
            var signaled = semaphore.Wait(timeout);
            
            if (Logger.IsTraceEnabled())
            {
                var message = signaled ? "Received signal" : "Timed out";
                Logger.Trace($"{message} waiting for '{resource}'... retrying");
            }
            return DateTime.UtcNow;
        }

        public void Release(string resource)
        {
            lock (_synRoot)
            {
                if(!_pool.TryGetValue(resource, out var semaphore) || semaphore.CurrentCount > 0)
                {
                    return;
                }
                semaphore.Release();
            }
            
            if(Logger.IsTraceEnabled())
            {
                Logger.Trace(
                $"Released Resource: '{resource}', for release " +
                $" Thread[{Thread.CurrentThread.ManagedThreadId}]");                
            }
        }
    }
}