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
            if (Logger.IsTraceEnabled())
            {
                Logger.Trace($"{resource} - Waiting {timeout}ms");    
            }
            var semaphore = _pool.GetOrAdd(resource, new SemaphoreSlim(0, 1));
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
            if(!_pool.TryGetValue(resource, out var semaphore) || semaphore.CurrentCount > 0)
            {
                return;
            }
            try
            {
                semaphore.Release();
            }
            catch (SemaphoreFullException)
            {
                Logger.Error($"Error releasing mutex for resource '{resource}' current count: {semaphore.CurrentCount}");
                throw;
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