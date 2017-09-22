using System;
using System.Reflection;
using System.Threading;
using Hangfire.Mongo.Signal.Mongo;
using Xunit.Sdk;

namespace Hangfire.Mongo.Tests.Utils
{
    public class MongoSignalAttribute : BeforeAfterTestAttribute
    {
        private static readonly object _globalLock = new object();

        private static long _count;
        private static CancellationTokenSource _cancellationTokenSource;

        public override void Before(MethodInfo methodUnderTest)
        {
            lock (_globalLock)
            {
                _count += 1;
                if (_count == 1)
                {
                    Start();
                }
            }
        }

        public override void After(MethodInfo methodUnderTest)
        {
            lock (_globalLock)
            {
                _count -= 1;
                if (_count == 0)
                {
                    Stop();
                }
            }
        }

        private void Start()
        {
            var waitHandle = new AutoResetEvent(false);
            _cancellationTokenSource = new CancellationTokenSource();
            var thread = new Thread(() =>
            {
                using (_cancellationTokenSource)
                {
                    try
                    {
                        var cancellationToken = _cancellationTokenSource.Token;
                        var signalCollection = ConnectionUtils.CreateStorage().Connection.Signal;
                        var mongoSignalManager = new MongoSignalBackgroundProcess(signalCollection);

                        while (!cancellationToken.IsCancellationRequested)
                        {
                            waitHandle.Set();
                            mongoSignalManager.Execute(cancellationToken);
                        }
                    }
                    catch (OperationCanceledException)
                    {
                        // Expected
                    }
                }
            });
            thread.Start();

            // Wait for the signal manager to be airborne
            waitHandle.WaitOne(TimeSpan.FromSeconds(5));
        }

        private void Stop()
        {
            _cancellationTokenSource.Cancel();
        }

    }
}
