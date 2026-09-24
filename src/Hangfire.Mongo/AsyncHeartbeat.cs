using System;
using System.Threading;
using System.Threading.Tasks;

namespace Hangfire.Mongo
{
    /// <summary>
    /// Invokes an asynchronous callback at a fixed interval until stopped.
    /// Unlike a <see cref="Timer"/> with a synchronous callback, no thread is blocked
    /// while the callback awaits I/O.
    /// </summary>
    internal sealed class AsyncHeartbeat
    {
        private readonly CancellationTokenSource _cancellation = new CancellationTokenSource();
        private int _stopped;

        private AsyncHeartbeat()
        {
        }

        /// <summary>
        /// Starts the heartbeat. The first beat happens after <paramref name="interval"/>.
        /// </summary>
        /// <param name="interval">Delay between beats</param>
        /// <param name="beat">Callback invoked on every beat</param>
        /// <param name="onError">Invoked when <paramref name="beat"/> throws; the heartbeat keeps running</param>
        public static AsyncHeartbeat Start(TimeSpan interval, Func<CancellationToken, Task> beat,
            Action<Exception> onError)
        {
            if (interval <= TimeSpan.Zero)
            {
                throw new ArgumentOutOfRangeException(nameof(interval), interval, "Interval must be positive");
            }

            if (beat == null) throw new ArgumentNullException(nameof(beat));
            if (onError == null) throw new ArgumentNullException(nameof(onError));

            var heartbeat = new AsyncHeartbeat();
            _ = RunAsync(interval, beat, onError, heartbeat._cancellation.Token);
            return heartbeat;
        }

        /// <summary>
        /// Stops the heartbeat and cancels a beat in progress. Safe to call more than once.
        /// A beat that is starting concurrently with this call may still run, so beats must
        /// be harmless after the owner has moved on.
        /// </summary>
        public void Stop()
        {
            if (Interlocked.Exchange(ref _stopped, 1) == 1)
            {
                return;
            }

            // The token is cancelled before the source is disposed, so the loop can still
            // observe it safely after disposal.
            _cancellation.Cancel();
            _cancellation.Dispose();
        }

        private static async Task RunAsync(TimeSpan interval, Func<CancellationToken, Task> beat,
            Action<Exception> onError, CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    await Task.Delay(interval, cancellationToken).ConfigureAwait(false);
                    await beat(cancellationToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    return;
                }
                catch (Exception ex)
                {
                    onError(ex);
                }
            }
        }
    }
}
