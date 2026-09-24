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
        private static readonly TimeSpan MinInterval = TimeSpan.FromMilliseconds(1);
        private static readonly TimeSpan MaxInterval = TimeSpan.FromMilliseconds(int.MaxValue);

        private readonly CancellationTokenSource _cancellation = new CancellationTokenSource();
        private int _stopped;

        private AsyncHeartbeat()
        {
        }

        /// <summary>
        /// True once <see cref="Stop"/> has been called
        /// </summary>
        public bool IsStopped => Volatile.Read(ref _stopped) == 1;

        /// <summary>
        /// Starts the heartbeat. The first beat happens after <paramref name="interval"/>.
        /// </summary>
        /// <param name="interval">Delay between beats, clamped to the range <see cref="Task.Delay(TimeSpan)"/> supports</param>
        /// <param name="beat">Callback invoked on every beat</param>
        /// <param name="onError">Invoked when <paramref name="beat"/> throws; the heartbeat keeps running</param>
        public static AsyncHeartbeat Start(TimeSpan interval, Func<Task> beat, Action<Exception> onError)
        {
            if (beat == null) throw new ArgumentNullException(nameof(beat));
            if (onError == null) throw new ArgumentNullException(nameof(onError));

            // Callers start the heartbeat after taking a lease, so an extreme interval must not throw here.
            if (interval < MinInterval) interval = MinInterval;
            if (interval > MaxInterval) interval = MaxInterval;

            var heartbeat = new AsyncHeartbeat();
            _ = RunAsync(interval, beat, onError, heartbeat._cancellation.Token);
            return heartbeat;
        }

        /// <summary>
        /// Stops further beats. Safe to call more than once. A beat that is already running
        /// is not interrupted and a beat starting concurrently may still run, so beats must be
        /// harmless after the owner has moved on.
        /// </summary>
        public void Stop()
        {
            if (Interlocked.Exchange(ref _stopped, 1) == 1)
            {
                return;
            }

            // Only Task.Delay observes the token; cancelling before disposing keeps it safe to read afterwards.
            _cancellation.Cancel();
            _cancellation.Dispose();
        }

        private static async Task RunAsync(TimeSpan interval, Func<Task> beat, Action<Exception> onError,
            CancellationToken cancellationToken)
        {
            while (true)
            {
                try
                {
                    await Task.Delay(interval, cancellationToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    return;
                }

                if (cancellationToken.IsCancellationRequested)
                {
                    return;
                }

                try
                {
                    await beat().ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    ReportError(onError, ex);
                }
            }
        }

        private static void ReportError(Action<Exception> onError, Exception exception)
        {
            try
            {
                onError(exception);
            }
            catch
            {
                // a failing error handler (e.g. logging) must not stop the heartbeat
            }
        }
    }
}
