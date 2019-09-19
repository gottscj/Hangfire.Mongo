using System;
using System.Collections.Generic;
using System.Threading;
using Hangfire.Logging;

namespace Hangfire.Mongo
{
    internal interface IJobQueueSemaphore
    {
        bool WaitAny(string[] queues, CancellationToken cancellationToken, TimeSpan timeout, out string queue, out bool timedOut);
        void Release(string queue);
        void WaitNonBlock(string queue);
    }
    internal sealed class JobQueueSemaphore : IJobQueueSemaphore, IDisposable
    {
        public static readonly IJobQueueSemaphore Instance = new JobQueueSemaphore();
        
        private static readonly ILog Logger = LogProvider.For<JobQueueSemaphore>();
        private readonly Dictionary<string, SemaphoreSlim> _pool = new Dictionary<string, SemaphoreSlim>();
        private readonly object _syncRoot = new object();
        
        public bool WaitAny(string[] queues, CancellationToken cancellationToken, TimeSpan timeout, out string queue, out bool timedOut)
        {
            queue = null;
            
            // wait for first item in queue
            var waitHandlers = GetWaitHandlers(queues, cancellationToken);
            var index = WaitHandle.WaitAny(waitHandlers, timeout);

            timedOut = index == WaitHandle.WaitTimeout;
            
            if (timedOut)
            {
                return false;
            }
            
            // check if cancellationTokens wait handle has been signaled
            if (index == (waitHandlers.Length - 1))
            {
                return false;
            }

            queue = queues[index];
            
            var semaphore = GetOrAddSemaphore(queue);
            
            
            
            var gotLock = semaphore.Wait(0, cancellationToken);
            
            if(gotLock && Logger.IsTraceEnabled())
            {
                Logger.Trace(
                $"Decremented semaphore (signalled) Queue: '{queue}', " +
                $"semaphore current count: {semaphore.CurrentCount}" +
                $" Thread[{Thread.CurrentThread.ManagedThreadId}]");                
            }
            // waithandle has been signaled. wait for the signaled semaphore to make sure its counter is decremented
            // https://docs.microsoft.com/en-us/dotnet/api/system.threading.semaphoreslim.availablewaithandle?view=netframework-4.7.2
            return gotLock;
        }

        public void Release(string queue)
        {
            var semaphore = GetOrAddSemaphore(queue);
            
            try
            {
                semaphore.Release();
            }
            catch (SemaphoreFullException)
            {
                Logger.Error($"Error releasing semaphore for queue '{queue}' current count: {semaphore.CurrentCount}");
                throw;
            }
            
            if (Logger.IsTraceEnabled())
            {
                Logger.Trace(
                    $"Incremented semaphore for Queue: '{queue}', " +
                    $"semaphore current count: {semaphore.CurrentCount}" +
                    $" Thread[{Thread.CurrentThread.ManagedThreadId}]");
            }
        }

        public void WaitNonBlock(string queue)
        {
            lock (_syncRoot)
            {
                if (_pool.TryGetValue(queue, out var semaphore) && semaphore.Wait(0))
                {
                    if (Logger.IsTraceEnabled())
                    {
                        Logger.Trace(
                            $"Decremented semaphore for Queue: '{queue}', " +
                            $"semaphore current count: {semaphore.CurrentCount}" +
                            $" Thread[{Thread.CurrentThread.ManagedThreadId}]");
                    }
                }
            }
            
        }

        private WaitHandle[] GetWaitHandlers(string[] queues, CancellationToken cancellationToken)
        {
            var waiters = new WaitHandle[queues.Length + 1];
            for (var i = 0; i < queues.Length; i++)
            {
                waiters[i] = GetOrAddSemaphore(queues[i]).AvailableWaitHandle; 
            }

            waiters[queues.Length] = cancellationToken.WaitHandle;

            return waiters;
        }

        private SemaphoreSlim GetOrAddSemaphore(string queue)
        {
            SemaphoreSlim semaphore;
            lock (_syncRoot)
            {
                if (!_pool.TryGetValue(queue, out semaphore))
                {
                    semaphore = new SemaphoreSlim(0);
                    _pool.Add(queue, semaphore);
                }
            }

            return semaphore;
        }
        
        public void Dispose()
        {
            Logger.Debug("Dispose");
            lock (_syncRoot)
            {
                foreach (var resetEvent in _pool.Values)
                {
                    resetEvent.Dispose();
                }
                _pool.Clear();
            }
            
        }
    }
}