using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading;
using Hangfire.Logging;

namespace Hangfire.Mongo
{
    internal interface IJobQueueSemaphore
    {
        int WaitAny(string[] queues, CancellationToken cancellationToken, TimeSpan timeout);
        void Release(string queue);
    }
    internal sealed class JobQueueSemaphore : IJobQueueSemaphore, IDisposable
    {
        public static IJobQueueSemaphore Default = new JobQueueSemaphore();
        
        private static readonly ILog Logger = LogProvider.For<JobQueueSemaphore>();
        private readonly ConcurrentDictionary<string, SemaphoreSlim> _pool = new ConcurrentDictionary<string, SemaphoreSlim>();

        public int WaitAny(string[] queues, CancellationToken cancellationToken, TimeSpan timeout)
        {
            var waitHandlers = GetWaitHandlers(queues, cancellationToken);
            var index = WaitHandle.WaitAny(waitHandlers, timeout);
           

            if (index == queues.Length)
            {
                // check if its the cancellation which is signaled 
                cancellationToken.ThrowIfCancellationRequested();
                return index; // should never get here.. ¯\_(ツ)_/¯
            }

            if (index == WaitHandle.WaitTimeout)
            {
                return index;
            }
            
            var queue = queues[index];
            var semaphore = _pool[queue];
            
            // waithandle has been signaled. wait for the signaled semaphore to make sure its counter is decremented
            // https://docs.microsoft.com/en-us/dotnet/api/system.threading.semaphoreslim.availablewaithandle?view=netframework-4.7.2
            semaphore.Wait(cancellationToken);
            Logger.Debug(
                $"Signal received for Queue: '{queue}', " +
                $"semaphore current count: {semaphore.CurrentCount}" +
                $" Thread[{Thread.CurrentThread.ManagedThreadId}]");

            return index;
        }
        
        public void Release(string queue)
        {
            _pool
                .GetOrAdd(queue, new SemaphoreSlim(0))
                .Release(1);
            
            Logger.Debug(
                $"Released semaphore for Queue: '{queue}', " +
                $"semaphore current count: {_pool[queue].CurrentCount}" +
                $" Thread[{Thread.CurrentThread.ManagedThreadId}]");
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