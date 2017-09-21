using System;
using System.Linq;
using System.Threading.Tasks;
using Hangfire.Common;
using Hangfire.Mongo.Tests.Utils;
using Xunit;

namespace Hangfire.Mongo.Tests
{
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
            var storage = ConnectionUtils.CreateStorage(new MongoStorageOptions());
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


        [Fact, CleanDatabase]
        public void MultipleBackgroundJobServers_AddsRecurrentJobs()
        {
            // ARRANGE
            const int serverCount = 15;
            const int workerCount = 2;

            JobStorage.Current = ConnectionUtils.CreateStorage(new MongoStorageOptions());

            var options = Enumerable.Range(0, serverCount)
                .Select((_, i) => new BackgroundJobServerOptions
                {
                    Queues = new[] { "default", $"queue_{i}" },
                    WorkerCount = workerCount
                })
                .ToList();

            var servers = options.Select(o => new BackgroundJobServer(o)).ToList();

            // let hangfire run for 1 sec
            Task.Delay(1000).Wait();

            // ACT
            foreach (var queue in options.SelectMany(o => o.Queues))
            {
                for (int i = 0; i < workerCount; i++)
                {
                    RecurringJob.AddOrUpdate($@"job_{queue}.{i}-a", () => Console.WriteLine($@"{queue}.{i}-a"), Cron.Minutely(), null, queue);
                    RecurringJob.AddOrUpdate($@"job_{queue}.{i}-b", () => Console.WriteLine($@"{queue}.{i}-b"), Cron.Minutely(), null, queue);
                }
            }

            // let hangfire run for 1 sec
            Task.Delay(1000).Wait();

            // ASSERT
            servers.ForEach(s =>
            {
                s.SendStop();
            });
            servers.ForEach(s =>
            {
                s.Dispose();
            });

        }

    }
}
