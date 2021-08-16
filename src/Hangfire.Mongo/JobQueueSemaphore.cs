using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Hangfire.Logging;

namespace Hangfire.Mongo
{
    /// <summary>
    /// Job queue semaphore waits for signal on jobs queued for given queues
    /// </summary>
    public interface IJobQueueSemaphore
    {
        /// <summary>
        /// Waits for signal on given queues
        /// </summary>
        /// <param name="queues"></param>
        /// <param name="cancellationToken"></param>
        /// <param name="timeout"></param>
        /// <param name="queue"></param>
        /// <param name="timedOut"></param>
        /// <returns></returns>
        bool WaitAny(string[] queues, CancellationToken cancellationToken, TimeSpan timeout, out string queue, out bool timedOut);
        
        /// <summary>
        /// Releases a semaphore for given queue
        /// </summary>
        /// <param name="queue"></param>
        void Release(string queue);
        
        /// <summary>
        /// Tries to release semaphore on given queue in a non-blocking call
        /// </summary>
        /// <param name="queue"></param>
        /// <returns></returns>
        bool WaitNonBlock(string queue);
    }

    /// <inheritdoc cref="IJobQueueSemaphore" />
    public class JobQueueSemaphore : IJobQueueSemaphore, IDisposable
    {
        private static readonly ILog Logger = LogProvider.For<JobQueueSemaphore>();
        private readonly Dictionary<string, long> _pool = new Dictionary<string, long>();
        private readonly ManualResetEvent _releasedSignal = new ManualResetEvent(false);
        private readonly object _syncRoot = new object();
        
        /// <inheritdoc />
        public virtual bool WaitAny(string[] queues, CancellationToken cancellationToken, TimeSpan timeout, out string queue, out bool timedOut)
        {
            lock (_syncRoot)
            {
                if (!queues.Any(_pool.ContainsKey) && _releasedSignal.WaitOne(0))
                {
                    // signalled but we dont know the queue, reset
                    _releasedSignal.Reset();   
                }
            }
            
            queue = null;
            
            // wait for first item in queue
            var index = WaitHandle.WaitAny(new []
            {
                _releasedSignal,
                cancellationToken.WaitHandle
            }, timeout);

            lock (_syncRoot)
            {
                timedOut = index == WaitHandle.WaitTimeout;
            
                if (timedOut)
                {
                    return false;
                }
                // check if cancellationTokens wait handle has been signaled
                if (index == 1)
                {
                    return false;
                }

                foreach (var q in queues)
                {
                    if (!WaitNonBlock(q))
                    {
                        continue;
                    }
                    queue = q;
                    break;
                }

                var allReleased = queues
                    .Where(_pool.ContainsKey)
                    .Select(q => _pool[q])
                    .All(i => i < 1);
                
                if (allReleased)
                {
                    _releasedSignal.Reset();
                }

                return !string.IsNullOrEmpty(queue);
            }
        }

        /// <inheritdoc />
        public virtual void Release(string queue)
        {
            long count;
            lock (_syncRoot)
            {
                _pool.TryGetValue(queue, out count);
                count += 1;
                _pool[queue] = count;
            }
            _releasedSignal.Set();

            if (Logger.IsTraceEnabled())
            {
                Logger.Trace(
                    $"Incremented semaphore for Queue: '{queue}', " +
                    $"semaphore current count: {count}" +
                    $" Thread[{Thread.CurrentThread.ManagedThreadId}]");
            }
        }

        /// <inheritdoc />
        public virtual bool WaitNonBlock(string queue)
        {
            lock (_syncRoot)
            {
                var gotLock = false;
                if (_pool.TryGetValue(queue, out var count) && count >= 1)
                {
                    count -= 1;
                    _pool[queue] = count;
                    gotLock = true;
                }

                if (gotLock && Logger.IsTraceEnabled())
                {
                    Logger.Trace(
                        $"Decremented semaphore for Queue: '{queue}', " +
                        $"semaphore current count: {count}" +
                        $" Thread[{Thread.CurrentThread.ManagedThreadId}]");
                }

                return gotLock;
            }
        }

        /// <summary>
        /// to string
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
            lock (_syncRoot)
            {
                return _pool.ToString();
            }
        }


        /// <inheritdoc />
        public void Dispose()
        {
            Logger.Debug("Dispose");
            lock (_syncRoot)
            {
                _releasedSignal.Dispose();
                _pool.Clear();
            }
            
        }
    }
}