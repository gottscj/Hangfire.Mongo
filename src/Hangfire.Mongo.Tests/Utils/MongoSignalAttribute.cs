using System;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Hangfire.Mongo.Signal.Mongo;
using Xunit.Sdk;

namespace Hangfire.Mongo.Tests.Utils
{
    public class MongoSignalAttribute : BeforeAfterTestAttribute
    {
        private static long _count = 0;
        private static readonly object _globalLock = new object();
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

        public void Start()
        {
            var waitHandle = new AutoResetEvent(false);
            _cancellationTokenSource = new CancellationTokenSource();
            Task.Run(() =>
            {
                using (_cancellationTokenSource)
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
            });

            // Wait for the signal manager to be airborne
            waitHandle.WaitOne(TimeSpan.FromSeconds(5));
        }

        public void Stop()
        {
            _cancellationTokenSource.Cancel();
        }

    }
}
