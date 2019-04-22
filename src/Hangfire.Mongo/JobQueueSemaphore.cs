using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading;
using Hangfire.Logging;

namespace Hangfire.Mongo
{
    internal interface IJobQueueSemaphore
    {
        string WaitAny(string[] queues, CancellationToken cancellationToken, TimeSpan timeout);
        void Release(string queue);
    }
    internal sealed class JobQueueSemaphore : IJobQueueSemaphore, IDisposable
    {
        public static readonly IJobQueueSemaphore Instance = new JobQueueSemaphore();
        
        private static readonly ILog Logger = LogProvider.For<JobQueueSemaphore>();
        private readonly ConcurrentDictionary<string, SemaphoreSlim> _pool = new ConcurrentDictionary<string, SemaphoreSlim>();

        public string WaitAny(string[] queues, CancellationToken cancellationToken, TimeSpan timeout)
        {
            var waitHandlers = GetWaitHandlers(queues, cancellationToken);
            var index = WaitHandle.WaitAny(waitHandlers, timeout);

            // check if cancellationTokens wait handle has been signaled
            if (index == (waitHandlers.Length - 1))
            {
                return null;
            }

            if (index == WaitHandle.WaitTimeout)
            {
                return null;
            }
            
            var queue = queues[index];
            var semaphore = _pool[queue];
            
            // waithandle has been signaled. wait for the signaled semaphore to make sure its counter is decremented
            // https://docs.microsoft.com/en-us/dotnet/api/system.threading.semaphoreslim.availablewaithandle?view=netframework-4.7.2
            semaphore.Wait(cancellationToken);
            if(Logger.IsDebugEnabled())
            {
                Logger.Debug(
                $"Signal received for Queue: '{queue}', " +
                $"semaphore current count: {semaphore.CurrentCount}" +
                $" Thread[{Thread.CurrentThread.ManagedThreadId}]");                
            }

            return queue;
        }
        
        public void Release(string queue)
        {
            _pool
                .GetOrAdd(queue, new SemaphoreSlim(0))
                .Release(1);
            if(Logger.IsDebugEnabled())
            {
                Logger.Debug(
                $"Released semaphore for Queue: '{queue}', " +
                $"semaphore current count: {_pool[queue].CurrentCount}" +
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