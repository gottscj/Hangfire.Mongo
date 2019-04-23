using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading;
using Hangfire.Logging;

namespace Hangfire.Mongo
{
    internal interface IJobQueueSemaphore
    {
        bool WaitAny(string[] queues, CancellationToken cancellationToken, TimeSpan timeout, out string queue);
        void WaitNonBlock(string queue);
        void Release(string queue);
    }
    internal sealed class JobQueueSemaphore : IJobQueueSemaphore, IDisposable
    {
        public static readonly IJobQueueSemaphore Instance = new JobQueueSemaphore();
        
        private static readonly ILog Logger = LogProvider.For<JobQueueSemaphore>();
        private readonly ConcurrentDictionary<string, SemaphoreSlim> _pool = new ConcurrentDictionary<string, SemaphoreSlim>();

        public bool WaitAny(string[] queues, CancellationToken cancellationToken, TimeSpan timeout, out string queue)
        {
            queue = null;
            
            // wait for first item in queue
            var waitHandlers = GetWaitHandlers(queues, cancellationToken);
            var index = WaitHandle.WaitAny(waitHandlers, timeout);

            // check if cancellationTokens wait handle has been signaled
            if (index == (waitHandlers.Length - 1))
            {
                return false;
            }

            if (index == WaitHandle.WaitTimeout)
            {
                return false;
            }
            
            queue = queues[index];
            var semaphore = _pool[queue];
            
            // waithandle has been signaled. wait for the signaled semaphore to make sure its counter is decremented
            // https://docs.microsoft.com/en-us/dotnet/api/system.threading.semaphoreslim.availablewaithandle?view=netframework-4.7.2
            semaphore.Wait(cancellationToken);
            if(Logger.IsTraceEnabled())
            {
                Logger.Trace(
                $"Decremented semaphore (signalled) Queue: '{queue}', " +
                $"semaphore current count: {semaphore.CurrentCount}" +
                $" Thread[{Thread.CurrentThread.ManagedThreadId}]");                
            }

            return true;
        }

        public void WaitNonBlock(string queue)
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

        public void Release(string queue)
        {
            var semaphore = _pool
                .GetOrAdd(queue, new SemaphoreSlim(0));

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

        private WaitHandle[] GetWaitHandlers(string[] queues, CancellationToken cancellationToken)
        {
            var waiters = new WaitHandle[queues.Length + 1];
            for (var i = 0; i < queues.Length; i++)
            {
                waiters[i] = _pool.GetOrAdd(queues[i], new SemaphoreSlim(0)).AvailableWaitHandle; 
            }

            waiters[queues.Length] = cancellationToken.WaitHandle;

            return waiters;
        }    
        
        public void Dispose()
        {
            Logger.Debug("Dispose");
            foreach (var queue in _pool.Keys.ToList())
            {
                if (_pool.TryRemove(queue, out var resetEvent))
                {
                    resetEvent.Dispose();
                }
            }
        }
    }
}