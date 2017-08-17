using System;
using System.Threading.Tasks;
using Hangfire.Common;
using Hangfire.Mongo.Tests.Utils;
using Xunit;

namespace Hangfire.Mongo.Tests
{
#pragma warning disable 1591
    [Collection("Database")]
    public class MultipleServersFacts
    {
        [Fact(Skip = "Long running and does not always fail"), CleanDatabase]
        public void MultipleServerRunsRecurrentJobs()
        {
            // ARRANGE
            const int serverCount = 20;
            const int workerCount = 20;

            var options = new BackgroundJobServerOptions[serverCount];
            var storage = ConnectionUtils.CreateStorage(new MongoStorageOptions { QueuePollInterval = TimeSpan.FromSeconds(1) });
            var servers = new BackgroundJobServer[serverCount];

            var jobManagers = new RecurringJobManager[serverCount];

            for (int i = 0; i < serverCount; i++)
            {
                options[i] = new BackgroundJobServerOptions { Queues = new[] { $"queue_options_{i}" }, WorkerCount = workerCount };

                servers[i] = new BackgroundJobServer(options[i], storage);
                jobManagers[i] = new RecurringJobManager(storage);
            }

            try
            {

                // ACT
                for (int i = 0; i < serverCount; i++)
                {
                    var i1 = i;
                    var jobManager = jobManagers[i1];

                    for (int j = 0; j < workerCount; j++)
                    {
                        var j1 = j;
                        var queueIndex = j1 % options[i1].Queues.Length;
                        var queueName = options[i1].Queues[queueIndex];
                        var job = Job.FromExpression(() => Console.WriteLine("Setting signal for queue {0}",
                            queueName));
                        var jobId = $"job:[{i},{j}]";

                        jobManager.AddOrUpdate(jobId, job, Cron.Minutely(), new RecurringJobOptions
                        {
                            QueueName = queueName
                        });
                        jobManager.Trigger(jobId);
                    }
                }

                // let hangfire run for 1 sec
                Task.Delay(1000).Wait();
            }
            finally
            {
                for (int i = 0; i < serverCount; i++)
                {
                    servers[i].SendStop();
                    servers[i].Dispose();
                }
            }
        }

    }
#pragma warning restore 1591
}
