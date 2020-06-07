using System;
using System.Collections.Generic;
using System.Threading;
using Hangfire.Logging;

namespace Hangfire.Mongo
{
    /// <summary>
    /// Mutex proxy for Hangfire locks
    /// </summary>
    public interface IDistributedLockMutex
    {
        /// <summary>
        /// Waits until signalled or timeout is reached
        /// </summary>
        /// <param name="resource"></param>
        /// <param name="timeout"></param>
        /// <returns></returns>
        DateTime Wait(string resource, TimeSpan timeout);
        
        /// <summary>
        /// Release lock
        /// </summary>
        /// <param name="resource"></param>
        void Release(string resource);
    }

    /// <inheritdoc />
    public class DistributedLockMutex : IDistributedLockMutex
    {
        private static readonly ILog Logger = LogProvider.For<DistributedLockMutex>();

        private readonly Dictionary<string, SemaphoreSlim> _pool = new Dictionary<string, SemaphoreSlim>();
        private readonly object _synRoot = new object();

        /// <inheritdoc />
        public virtual DateTime Wait(string resource, TimeSpan timeout)
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

        /// <inheritdoc />
        public virtual void Release(string resource)
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