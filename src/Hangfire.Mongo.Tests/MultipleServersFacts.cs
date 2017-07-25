using System;
using System.Diagnostics;
using System.Threading;
using Hangfire.Common;
using Hangfire.Mongo.Tests.Utils;
using Xunit;

namespace Hangfire.Mongo.Tests
{
#pragma warning disable 1591
    [Collection("Database")]
    public class MultipleServersFacts
    {
        private static int _serverCount = 5;
        private static int _workerCount = 20;
        private static AutoResetEvent[] _signals;

        [/*Fact*/ Fact(Skip = "Long running and does not always fail"), CleanDatabase]
        public void MultipleServerRunsRecurrentJobs()
        {
            // ARRANGE

            var options = new BackgroundJobServerOptions[_serverCount];
            var storage = new MongoStorage[_serverCount];
            var servers = new BackgroundJobServer[_serverCount];
            _signals = new AutoResetEvent[_serverCount];
            var jobManagers = new RecurringJobManager[_serverCount];

            for (int i = 0; i < _serverCount; i++)
            {
                options[i] = new BackgroundJobServerOptions {Queues = new[] {$"queue_options_{i}"}, WorkerCount = _workerCount };
                storage[i] = new MongoStorage(ConnectionUtils.GetConnectionString(), ConnectionUtils.GetDatabaseName(),
                    new MongoStorageOptions {QueuePollInterval = TimeSpan.FromSeconds(1)});

                servers[i] = new BackgroundJobServer(options[i], storage[i]);
                jobManagers[i] = new RecurringJobManager(storage[i]);
                _signals[i] = new AutoResetEvent(false);
            }
            try
            {
                // ACT
                for (int i = 0; i < _serverCount; i++)
                {
                    var i1 = i;
                    var jobManager = jobManagers[i1];
                    
                    for (int j = 0; j < _workerCount; j++)
                    {
                        var j1 = j;
                        var queueIndex = j1 % options[i1].Queues.Length;
                        var queueName = options[i1].Queues[queueIndex];
                        var job = Job.FromExpression(() => SetSignal(queueName, i1));
                        var jobId = Guid.NewGuid().ToString("N");

                        jobManager.AddOrUpdate(jobId, job, Cron.Minutely(), new RecurringJobOptions
                        {
                            QueueName = queueName
                        });
                    }
                }
                WaitHandle.WaitAll(_signals);
                // ASSERT
                // no exceptions
            }
            finally
            {
                for (int i = 0; i < _serverCount; i++)
                {
                    servers[i].SendStop();
                    servers[i].Dispose();
                    _signals[i].Dispose();
                }
            }
        }

        public static void SetSignal(string queueName, int index)
        {
            Debug.WriteLine("Setting signal for queue {0}", queueName);
            _signals[index].Set();
        }
    }
#pragma warning restore 1591
}
