using System;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace Hangfire.Mongo.Tests
{
#pragma warning disable 1591
    public class AsyncHeartbeatFacts
    {
        private static readonly TimeSpan Interval = TimeSpan.FromMilliseconds(50);

        [Fact]
        public void Start_ThrowsAnException_WhenIntervalIsNotPositive()
        {
            Assert.Throws<ArgumentOutOfRangeException>(
                () => AsyncHeartbeat.Start(TimeSpan.Zero, () => Task.CompletedTask, _ => { }));
        }

        [Fact]
        public async Task Start_BeatsRepeatedly_UntilStopped()
        {
            var beats = 0;
            var heartbeat = AsyncHeartbeat.Start(Interval, () =>
            {
                Interlocked.Increment(ref beats);
                return Task.CompletedTask;
            }, _ => { });

            Assert.True(await WaitUntil(() => Volatile.Read(ref beats) >= 3), "Expected at least 3 beats");

            heartbeat.Stop();
            // a beat racing with Stop may still complete
            await Task.Delay(TimeSpan.FromMilliseconds(500));
            var beatsAfterStop = Volatile.Read(ref beats);
            await Task.Delay(Interval * 4);

            Assert.Equal(beatsAfterStop, Volatile.Read(ref beats));
        }

        [Fact]
        public async Task Stop_BeforeFirstBeat_PreventsAnyBeat()
        {
            var interval = TimeSpan.FromMilliseconds(500);
            var beats = 0;
            var heartbeat = AsyncHeartbeat.Start(interval, () =>
            {
                Interlocked.Increment(ref beats);
                return Task.CompletedTask;
            }, _ => { });

            heartbeat.Stop();
            await Task.Delay(interval * 2);

            Assert.Equal(0, Volatile.Read(ref beats));
        }

        [Fact]
        public void Stop_CanBeCalledMoreThanOnce()
        {
            var heartbeat = AsyncHeartbeat.Start(Interval, () => Task.CompletedTask, _ => { });

            heartbeat.Stop();
            heartbeat.Stop();
        }

        [Fact]
        public async Task Stop_DoesNotInterruptBeatInProgress_AndPreventsFurtherBeats()
        {
            var beats = 0;
            var errors = 0;
            var beatStarted = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            var releaseBeat = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            var heartbeat = AsyncHeartbeat.Start(Interval, async () =>
            {
                Interlocked.Increment(ref beats);
                beatStarted.TrySetResult(true);
                await releaseBeat.Task;
            }, _ => Interlocked.Increment(ref errors));

            await beatStarted.Task.WaitAsync(TimeSpan.FromSeconds(5));
            heartbeat.Stop();
            Assert.True(heartbeat.IsStopped);
            releaseBeat.SetResult(true);
            await Task.Delay(Interval * 4);

            Assert.Equal(1, Volatile.Read(ref beats));
            Assert.Equal(0, Volatile.Read(ref errors));
        }

        [Fact]
        public void Start_ThrowsAnException_WhenIntervalIsTooLarge()
        {
            Assert.Throws<ArgumentOutOfRangeException>(
                () => AsyncHeartbeat.Start(TimeSpan.FromMilliseconds(int.MaxValue + 1L), () => Task.CompletedTask, _ => { }));
        }

        [Fact]
        public async Task Beat_KeepsRunning_WhenErrorHandlerThrows()
        {
            var beats = 0;
            var heartbeat = AsyncHeartbeat.Start(Interval, () =>
            {
                Interlocked.Increment(ref beats);
                throw new InvalidOperationException("boom");
            }, _ => throw new InvalidOperationException("logging failed"));

            try
            {
                Assert.True(await WaitUntil(() => Volatile.Read(ref beats) >= 3), "Expected at least 3 beats");
            }
            finally
            {
                heartbeat.Stop();
            }
        }

        [Fact]
        public async Task Beat_KeepsRunning_WhenCallbackThrows()
        {
            var beats = 0;
            var errors = 0;
            var heartbeat = AsyncHeartbeat.Start(Interval, () =>
            {
                Interlocked.Increment(ref beats);
                throw new InvalidOperationException("boom");
            }, ex =>
            {
                Assert.IsType<InvalidOperationException>(ex);
                Interlocked.Increment(ref errors);
            });

            try
            {
                Assert.True(await WaitUntil(() => Volatile.Read(ref errors) >= 3), "Expected at least 3 reported errors");
                Assert.True(Volatile.Read(ref beats) >= 3);
            }
            finally
            {
                heartbeat.Stop();
            }
        }

        [Fact]
        public async Task Stop_FromWithinBeat_StopsFurtherBeats()
        {
            var beats = 0;
            AsyncHeartbeat heartbeat = null;
            var started = new ManualResetEventSlim();
            heartbeat = AsyncHeartbeat.Start(Interval, () =>
            {
                started.Wait();
                Interlocked.Increment(ref beats);
                heartbeat.Stop();
                return Task.CompletedTask;
            }, _ => { });
            started.Set();

            Assert.True(await WaitUntil(() => Volatile.Read(ref beats) == 1), "Expected one beat");
            await Task.Delay(Interval * 4);

            Assert.Equal(1, Volatile.Read(ref beats));
        }

        private static async Task<bool> WaitUntil(Func<bool> condition)
        {
            var deadline = DateTime.UtcNow.AddSeconds(5);
            while (DateTime.UtcNow < deadline)
            {
                if (condition())
                {
                    return true;
                }

                await Task.Delay(10);
            }

            return condition();
        }
    }
#pragma warning restore 1591
}
