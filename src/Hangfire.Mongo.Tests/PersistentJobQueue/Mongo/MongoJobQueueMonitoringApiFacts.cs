using System;
using System.Linq;
using Hangfire.Mongo.Database;
using Hangfire.Mongo.Dto;
using Hangfire.Mongo.PersistentJobQueue.Mongo;
using Hangfire.Mongo.Tests.Utils;
using Xunit;

namespace Hangfire.Mongo.Tests.PersistentJobQueue.Mongo
{
    [Collection("Database")]
    public class MongoJobQueueMonitoringApiFacts
    {
        private const string QueueName1 = "queueName1";
        private const string QueueName2 = "queueName2";

        [Fact]
        public void Ctor_ThrowsAnException_WhenConnectionIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(() => new MongoJobQueueMonitoringApi(null));

            Assert.Equal("database", exception.ParamName);
        }

        [Fact, CleanDatabase]
        public void GetQueues_ShouldReturnEmpty_WhenNoQueuesExist()
        {
            ConnectionUtils.UseConnection(database =>
            {
                var mongoJobQueueMonitoringApi = CreateMongoJobQueueMonitoringApi(database);

                var queues = mongoJobQueueMonitoringApi.GetQueues();

                Assert.Empty(queues);
            });
        }

        [Fact, CleanDatabase]
        public void GetQueues_ShouldReturnOneQueue_WhenOneQueueExists()
        {
            ConnectionUtils.UseConnection(database =>
            {
                var mongoJobQueueMonitoringApi = CreateMongoJobQueueMonitoringApi(database);

                CreateJobQueueDto(database, QueueName1, false);

                var queues = mongoJobQueueMonitoringApi.GetQueues().ToList();

                Assert.Single(queues);
                Assert.Equal(QueueName1, queues.First());
            });
        }

        [Fact, CleanDatabase]
        public void GetQueues_ShouldReturnTwoUniqueQueues_WhenThreeNonUniqueQueuesExist()
        {
            ConnectionUtils.UseConnection(database =>
            {
                var mongoJobQueueMonitoringApi = CreateMongoJobQueueMonitoringApi(database);

                CreateJobQueueDto(database, QueueName1, false);
                CreateJobQueueDto(database, QueueName1, false);
                CreateJobQueueDto(database, QueueName2, false);

                var queues = mongoJobQueueMonitoringApi.GetQueues().ToList();

                Assert.Equal(2, queues.Count);
                Assert.Contains(QueueName1, queues);
                Assert.Contains(QueueName2, queues);
            });
        }

        [Fact, CleanDatabase]
        public void GetEnqueuedJobIds_ShouldReturnEmpty_WheNoQueuesExist()
        {
            ConnectionUtils.UseConnection(database =>
            {
                var mongoJobQueueMonitoringApi = CreateMongoJobQueueMonitoringApi(database);

                var enqueuedJobIds = mongoJobQueueMonitoringApi.GetEnqueuedJobIds(QueueName1, 0, 10);

                Assert.Empty(enqueuedJobIds);
            });
        }

        [Fact, CleanDatabase]
        public void GetEnqueuedJobIds_ShouldReturnEmpty_WhenOneJobWithAFetchedStateExists()
        {
            ConnectionUtils.UseConnection(database =>
            {
                var mongoJobQueueMonitoringApi = CreateMongoJobQueueMonitoringApi(database);

                CreateJobQueueDto(database, QueueName1, true);

                var enqueuedJobIds = mongoJobQueueMonitoringApi.GetEnqueuedJobIds(QueueName1, 0, 10).ToList();

                Assert.Empty(enqueuedJobIds);
            });
        }

        [Fact, CleanDatabase]
        public void GetEnqueuedJobIds_ShouldReturnOneJobId_WhenOneJobExists()
        {
            ConnectionUtils.UseConnection(database =>
            {
                var mongoJobQueueMonitoringApi = CreateMongoJobQueueMonitoringApi(database);

                var jobQueueDto = CreateJobQueueDto(database, QueueName1, false);

                var enqueuedJobIds = mongoJobQueueMonitoringApi.GetEnqueuedJobIds(QueueName1, 0, 10).ToList();

                Assert.Single(enqueuedJobIds);
                Assert.Equal(jobQueueDto.JobId.ToString(), enqueuedJobIds.First());
            });
        }

        [Fact, CleanDatabase]
        public void GetEnqueuedJobIds_ShouldReturnThreeJobIds_WhenThreeJobsExists()
        {
            ConnectionUtils.UseConnection(database =>
            {
                var mongoJobQueueMonitoringApi = CreateMongoJobQueueMonitoringApi(database);

                var jobQueueDto1 = CreateJobQueueDto(database, QueueName1, false);
                var jobQueueDto2 = CreateJobQueueDto(database, QueueName1, false);
                var jobQueueDto3 = CreateJobQueueDto(database, QueueName1, false);

                var enqueuedJobIds = mongoJobQueueMonitoringApi.GetEnqueuedJobIds(QueueName1, 0, 10).ToList();

                Assert.Equal(3, enqueuedJobIds.Count);
                Assert.Contains(jobQueueDto1.JobId.ToString(), enqueuedJobIds);
                Assert.Contains(jobQueueDto2.JobId.ToString(), enqueuedJobIds);
                Assert.Contains(jobQueueDto3.JobId.ToString(), enqueuedJobIds);
            });
        }

        [Fact, CleanDatabase]
        public void GetEnqueuedJobIds_ShouldReturnTwoJobIds_WhenThreeJobsExistsButOnlyTwoInRequestedQueue()
        {
            ConnectionUtils.UseConnection(database =>
            {
                var mongoJobQueueMonitoringApi = CreateMongoJobQueueMonitoringApi(database);

                var jobQueueDto1 = CreateJobQueueDto(database, QueueName1, false);
                var jobQueueDto2 = CreateJobQueueDto(database, QueueName1, false);
                CreateJobQueueDto(database, QueueName2, false);

                var enqueuedJobIds = mongoJobQueueMonitoringApi.GetEnqueuedJobIds(QueueName1, 0, 10).ToList();

                Assert.Equal(2, enqueuedJobIds.Count);
                Assert.Contains(jobQueueDto1.JobId.ToString(), enqueuedJobIds);
                Assert.Contains(jobQueueDto2.JobId.ToString(), enqueuedJobIds);
            });
        }

        [Fact, CleanDatabase]
        public void GetEnqueuedJobIds_ShouldReturnTwoJobIds_WhenThreeJobsExistsButLimitIsSet()
        {
            ConnectionUtils.UseConnection(database =>
            {
                var mongoJobQueueMonitoringApi = CreateMongoJobQueueMonitoringApi(database);

                var jobQueueDto1 = CreateJobQueueDto(database, QueueName1, false);
                var jobQueueDto2 = CreateJobQueueDto(database, QueueName1, false);
                CreateJobQueueDto(database, QueueName1, false);

                var enqueuedJobIds = mongoJobQueueMonitoringApi.GetEnqueuedJobIds(QueueName1, 0, 2).ToList();

                Assert.Equal(2, enqueuedJobIds.Count);
                Assert.Contains(jobQueueDto1.JobId.ToString(), enqueuedJobIds);
                Assert.Contains(jobQueueDto2.JobId.ToString(), enqueuedJobIds);
            });
        }

        [Fact, CleanDatabase]
        public void GetFetchedJobIds_ShouldReturnEmpty_WheNoQueuesExist()
        {
            ConnectionUtils.UseConnection(database =>
            {
                var mongoJobQueueMonitoringApi = CreateMongoJobQueueMonitoringApi(database);

                var enqueuedJobIds = mongoJobQueueMonitoringApi.GetFetchedJobIds(QueueName1, 0, 10);

                Assert.Empty(enqueuedJobIds);
            });
        }

        [Fact, CleanDatabase]
        public void GetFetchedJobIds_ShouldReturnEmpty_WhenOneJobWithNonFetchedStateExists()
        {
            ConnectionUtils.UseConnection(database =>
            {
                var mongoJobQueueMonitoringApi = CreateMongoJobQueueMonitoringApi(database);

                CreateJobQueueDto(database, QueueName1, false);

                var enqueuedJobIds = mongoJobQueueMonitoringApi.GetFetchedJobIds(QueueName1, 0, 10).ToList();

                Assert.Empty(enqueuedJobIds);
            });
        }

        [Fact, CleanDatabase]
        public void GetFetchedJobIds_ShouldReturnOneJobId_WhenOneJobExists()
        {
            ConnectionUtils.UseConnection(database =>
            {
                var mongoJobQueueMonitoringApi = CreateMongoJobQueueMonitoringApi(database);

                var jobQueueDto = CreateJobQueueDto(database, QueueName1, true);

                var enqueuedJobIds = mongoJobQueueMonitoringApi.GetFetchedJobIds(QueueName1, 0, 10).ToList();

                Assert.Single(enqueuedJobIds);
                Assert.Equal(jobQueueDto.JobId.ToString(), enqueuedJobIds.First());
            });
        }

        [Fact, CleanDatabase]
        public void GetFetchedJobIds_ShouldReturnThreeJobIds_WhenThreeJobsExists()
        {
            ConnectionUtils.UseConnection(database =>
            {
                var mongoJobQueueMonitoringApi = CreateMongoJobQueueMonitoringApi(database);

                var jobQueueDto1 = CreateJobQueueDto(database, QueueName1, true);
                var jobQueueDto2 = CreateJobQueueDto(database, QueueName1, true);
                var jobQueueDto3 = CreateJobQueueDto(database, QueueName1, true);

                var enqueuedJobIds = mongoJobQueueMonitoringApi.GetFetchedJobIds(QueueName1, 0, 10).ToList();

                Assert.Equal(3, enqueuedJobIds.Count);
                Assert.Contains(jobQueueDto1.JobId.ToString(), enqueuedJobIds);
                Assert.Contains(jobQueueDto2.JobId.ToString(), enqueuedJobIds);
                Assert.Contains(jobQueueDto3.JobId.ToString(), enqueuedJobIds);
            });
        }

        [Fact, CleanDatabase]
        public void GetFetchedJobIds_ShouldReturnTwoJobIds_WhenThreeJobsExistsButOnlyTwoInRequestedQueue()
        {
            ConnectionUtils.UseConnection(database =>
            {
                var mongoJobQueueMonitoringApi = CreateMongoJobQueueMonitoringApi(database);

                var jobQueueDto1 = CreateJobQueueDto(database, QueueName1, true);
                var jobQueueDto2 = CreateJobQueueDto(database, QueueName1, true);
                CreateJobQueueDto(database, QueueName2, true);

                var enqueuedJobIds = mongoJobQueueMonitoringApi.GetFetchedJobIds(QueueName1, 0, 10).ToList();

                Assert.Equal(2, enqueuedJobIds.Count);
                Assert.Contains(jobQueueDto1.JobId.ToString(), enqueuedJobIds);
                Assert.Contains(jobQueueDto2.JobId.ToString(), enqueuedJobIds);
            });
        }

        [Fact, CleanDatabase]
        public void GetFetchedJobIds_ShouldReturnTwoJobIds_WhenThreeJobsExistsButLimitIsSet()
        {
            ConnectionUtils.UseConnection(database =>
            {
                var mongoJobQueueMonitoringApi = CreateMongoJobQueueMonitoringApi(database);

                var jobQueueDto1 = CreateJobQueueDto(database, QueueName1, true);
                var jobQueueDto2 = CreateJobQueueDto(database, QueueName1, true);
                CreateJobQueueDto(database, QueueName1, true);

                var enqueuedJobIds = mongoJobQueueMonitoringApi.GetFetchedJobIds(QueueName1, 0, 2).ToList();

                Assert.Equal(2, enqueuedJobIds.Count);
                Assert.Contains(jobQueueDto1.JobId.ToString(), enqueuedJobIds);
                Assert.Contains(jobQueueDto2.JobId.ToString(), enqueuedJobIds);
            });
        }

        private static JobQueueDto CreateJobQueueDto(HangfireDbContext database, string queue, bool isFetched)
        {
            var job = new JobDto
            {
                CreatedAt = DateTime.UtcNow,
                StateHistory = new[] { new StateDto() }
            };

            database.Job.InsertOne(job);

            var jobQueue = new JobQueueDto
            {
                Queue = queue,
                JobId = job.Id
            };

            if (isFetched)
            {
                jobQueue.FetchedAt = DateTime.UtcNow.AddDays(-1);
            }

            database.JobQueue.InsertOne(jobQueue);

            return jobQueue;
        }

        private static MongoJobQueueMonitoringApi CreateMongoJobQueueMonitoringApi(HangfireDbContext database)
        {
            return new MongoJobQueueMonitoringApi(database);
        }

    }
}
