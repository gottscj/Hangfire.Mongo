using System;
using System.Linq;
using Hangfire.Mongo.Database;
using Hangfire.Mongo.Dto;
using Hangfire.Mongo.PersistentJobQueue.Mongo;
using Hangfire.Mongo.Tests.Utils;
using Xunit;

namespace Hangfire.Mongo.Tests.PersistentJobQueue.Mongo
{
#pragma warning disable 1591
    [Collection("Database")]
    public class MongoJobQueueMonitoringApiFacts
    {
        private const string QueueName1 = "queueName1";
        private const string QueueName2 = "queueName2";

        [Fact]
        public void Ctor_ThrowsAnException_WhenConnectionIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(() => new MongoJobQueueMonitoringApi(null));

            Assert.Equal("connection", exception.ParamName);
        }

        [Fact, CleanDatabase]
        public void GetQueues_ShouldReturnEmpty_WhenNoQueuesExist()
        {
            UseConnection(connection =>
            {
                var mongoJobQueueMonitoringApi = CreateMongoJobQueueMonitoringApi(connection);

                var queues = mongoJobQueueMonitoringApi.GetQueues();

                Assert.Empty(queues);
            });
        }

        [Fact, CleanDatabase]
        public void GetQueues_ShouldReturnOneQueue_WhenOneQueueExists()
        {
            UseConnection(connection =>
            {
                var mongoJobQueueMonitoringApi = CreateMongoJobQueueMonitoringApi(connection);

                CreateJobQueueDto(connection, QueueName1, false);

                var queues = mongoJobQueueMonitoringApi.GetQueues().ToList();

                Assert.Equal(1, queues.Count);
                Assert.Equal(QueueName1, queues.First());
            });
        }

        [Fact, CleanDatabase]
        public void GetQueues_ShouldReturnTwoUniqueQueues_WhenThreeNonUniqueQueuesExist()
        {
            UseConnection(connection =>
            {
                var mongoJobQueueMonitoringApi = CreateMongoJobQueueMonitoringApi(connection);

                CreateJobQueueDto(connection, QueueName1, false);
                CreateJobQueueDto(connection, QueueName1, false);
                CreateJobQueueDto(connection, QueueName2, false);

                var queues = mongoJobQueueMonitoringApi.GetQueues().ToList();

                Assert.Equal(2, queues.Count);
                Assert.True(queues.Contains(QueueName1));
                Assert.True(queues.Contains(QueueName2));
            });
        }

        [Fact, CleanDatabase]
        public void GetEnqueuedJobIds_ShouldReturnEmpty_WheNoQueuesExist()
        {
            UseConnection(connection =>
            {
                var mongoJobQueueMonitoringApi = CreateMongoJobQueueMonitoringApi(connection);

                var enqueuedJobIds = mongoJobQueueMonitoringApi.GetEnqueuedJobIds(QueueName1, 0, 10);

                Assert.Empty(enqueuedJobIds);
            });
        }

        [Fact, CleanDatabase]
        public void GetEnqueuedJobIds_ShouldReturnEmpty_WhenOneJobWithAFetchedStateExists()
        {
            UseConnection(connection =>
            {
                var mongoJobQueueMonitoringApi = CreateMongoJobQueueMonitoringApi(connection);

                CreateJobQueueDto(connection, QueueName1, true);

                var enqueuedJobIds = mongoJobQueueMonitoringApi.GetEnqueuedJobIds(QueueName1, 0, 10).ToList();

                Assert.Empty(enqueuedJobIds);
            });
        }

        [Fact, CleanDatabase]
        public void GetEnqueuedJobIds_ShouldReturnOneJobId_WhenOneJobExists()
        {
            UseConnection(connection =>
            {
                var mongoJobQueueMonitoringApi = CreateMongoJobQueueMonitoringApi(connection);

                var jobQueueDto = CreateJobQueueDto(connection, QueueName1, false);

                var enqueuedJobIds = mongoJobQueueMonitoringApi.GetEnqueuedJobIds(QueueName1, 0, 10).ToList();

                Assert.Equal(1, enqueuedJobIds.Count);
                Assert.Equal(jobQueueDto.JobId.ToString(), enqueuedJobIds.First());
            });
        }

        [Fact, CleanDatabase]
        public void GetEnqueuedJobIds_ShouldReturnThreeJobIds_WhenThreeJobsExists()
        {
            UseConnection(connection =>
            {
                var mongoJobQueueMonitoringApi = CreateMongoJobQueueMonitoringApi(connection);

                var jobQueueDto = CreateJobQueueDto(connection, QueueName1, false);
                var jobQueueDto2 = CreateJobQueueDto(connection, QueueName1, false);
                var jobQueueDto3 = CreateJobQueueDto(connection, QueueName1, false);

                var enqueuedJobIds = mongoJobQueueMonitoringApi.GetEnqueuedJobIds(QueueName1, 0, 10).ToList();

                Assert.Equal(3, enqueuedJobIds.Count);
                Assert.True(enqueuedJobIds.Contains(jobQueueDto.JobId.ToString()));
                Assert.True(enqueuedJobIds.Contains(jobQueueDto2.JobId.ToString()));
                Assert.True(enqueuedJobIds.Contains(jobQueueDto3.JobId.ToString()));
            });
        }

        [Fact, CleanDatabase]
        public void GetEnqueuedJobIds_ShouldReturnTwoJobIds_WhenThreeJobsExistsButOnlyTwoInRequestedQueue()
        {
            UseConnection(connection =>
            {
                var mongoJobQueueMonitoringApi = CreateMongoJobQueueMonitoringApi(connection);

                var jobQueueDto = CreateJobQueueDto(connection, QueueName1, false);
                var jobQueueDto2 = CreateJobQueueDto(connection, QueueName1, false);
                CreateJobQueueDto(connection, QueueName2, false);

                var enqueuedJobIds = mongoJobQueueMonitoringApi.GetEnqueuedJobIds(QueueName1, 0, 10).ToList();

                Assert.Equal(2, enqueuedJobIds.Count);
                Assert.True(enqueuedJobIds.Contains(jobQueueDto.JobId.ToString()));
                Assert.True(enqueuedJobIds.Contains(jobQueueDto2.JobId.ToString()));
            });
        }

        [Fact, CleanDatabase]
        public void GetEnqueuedJobIds_ShouldReturnTwoJobIds_WhenThreeJobsExistsButLimitIsSet()
        {
            UseConnection(connection =>
            {
                var mongoJobQueueMonitoringApi = CreateMongoJobQueueMonitoringApi(connection);

                var jobQueueDto = CreateJobQueueDto(connection, QueueName1, false);
                var jobQueueDto2 = CreateJobQueueDto(connection, QueueName1, false);
                CreateJobQueueDto(connection, QueueName1, false);

                var enqueuedJobIds = mongoJobQueueMonitoringApi.GetEnqueuedJobIds(QueueName1, 0, 2).ToList();

                Assert.Equal(2, enqueuedJobIds.Count);
                Assert.True(enqueuedJobIds.Contains(jobQueueDto.JobId.ToString()));
                Assert.True(enqueuedJobIds.Contains(jobQueueDto2.JobId.ToString()));
            });
        }

        [Fact, CleanDatabase]
        public void GetFetchedJobIds_ShouldReturnEmpty_WheNoQueuesExist()
        {
            UseConnection(connection =>
            {
                var mongoJobQueueMonitoringApi = CreateMongoJobQueueMonitoringApi(connection);

                var enqueuedJobIds = mongoJobQueueMonitoringApi.GetFetchedJobIds(QueueName1, 0, 10);

                Assert.Empty(enqueuedJobIds);
            });
        }

        [Fact, CleanDatabase]
        public void GetFetchedJobIds_ShouldReturnEmpty_WhenOneJobWithNonFetchedStateExists()
        {
            UseConnection(connection =>
            {
                var mongoJobQueueMonitoringApi = CreateMongoJobQueueMonitoringApi(connection);

                CreateJobQueueDto(connection, QueueName1, false);

                var enqueuedJobIds = mongoJobQueueMonitoringApi.GetFetchedJobIds(QueueName1, 0, 10).ToList();

                Assert.Empty(enqueuedJobIds);
            });
        }

        [Fact, CleanDatabase]
        public void GetFetchedJobIds_ShouldReturnOneJobId_WhenOneJobExists()
        {
            UseConnection(connection =>
            {
                var mongoJobQueueMonitoringApi = CreateMongoJobQueueMonitoringApi(connection);

                var jobQueueDto = CreateJobQueueDto(connection, QueueName1, true);

                var enqueuedJobIds = mongoJobQueueMonitoringApi.GetFetchedJobIds(QueueName1, 0, 10).ToList();

                Assert.Equal(1, enqueuedJobIds.Count);
                Assert.Equal(jobQueueDto.JobId.ToString(), enqueuedJobIds.First());
            });
        }

        [Fact, CleanDatabase]
        public void GetFetchedJobIds_ShouldReturnThreeJobIds_WhenThreeJobsExists()
        {
            UseConnection(connection =>
            {
                var mongoJobQueueMonitoringApi = CreateMongoJobQueueMonitoringApi(connection);

                var jobQueueDto = CreateJobQueueDto(connection, QueueName1, true);
                var jobQueueDto2 = CreateJobQueueDto(connection, QueueName1, true);
                var jobQueueDto3 = CreateJobQueueDto(connection, QueueName1, true);

                var enqueuedJobIds = mongoJobQueueMonitoringApi.GetFetchedJobIds(QueueName1, 0, 10).ToList();

                Assert.Equal(3, enqueuedJobIds.Count);
                Assert.True(enqueuedJobIds.Contains(jobQueueDto.JobId.ToString()));
                Assert.True(enqueuedJobIds.Contains(jobQueueDto2.JobId.ToString()));
                Assert.True(enqueuedJobIds.Contains(jobQueueDto3.JobId.ToString()));
            });
        }

        [Fact, CleanDatabase]
        public void GetFetchedJobIds_ShouldReturnTwoJobIds_WhenThreeJobsExistsButOnlyTwoInRequestedQueue()
        {
            UseConnection(connection =>
            {
                var mongoJobQueueMonitoringApi = CreateMongoJobQueueMonitoringApi(connection);

                var jobQueueDto = CreateJobQueueDto(connection, QueueName1, true);
                var jobQueueDto2 = CreateJobQueueDto(connection, QueueName1, true);
                CreateJobQueueDto(connection, QueueName2, true);

                var enqueuedJobIds = mongoJobQueueMonitoringApi.GetFetchedJobIds(QueueName1, 0, 10).ToList();

                Assert.Equal(2, enqueuedJobIds.Count);
                Assert.True(enqueuedJobIds.Contains(jobQueueDto.JobId.ToString()));
                Assert.True(enqueuedJobIds.Contains(jobQueueDto2.JobId.ToString()));
            });
        }

        [Fact, CleanDatabase]
        public void GetFetchedJobIds_ShouldReturnTwoJobIds_WhenThreeJobsExistsButLimitIsSet()
        {
            UseConnection(connection =>
            {
                var mongoJobQueueMonitoringApi = CreateMongoJobQueueMonitoringApi(connection);

                var jobQueueDto = CreateJobQueueDto(connection, QueueName1, true);
                var jobQueueDto2 = CreateJobQueueDto(connection, QueueName1, true);
                CreateJobQueueDto(connection, QueueName1, true);

                var enqueuedJobIds = mongoJobQueueMonitoringApi.GetFetchedJobIds(QueueName1, 0, 2).ToList();

                Assert.Equal(2, enqueuedJobIds.Count);
                Assert.True(enqueuedJobIds.Contains(jobQueueDto.JobId.ToString()));
                Assert.True(enqueuedJobIds.Contains(jobQueueDto2.JobId.ToString()));
            });
        }

        private static JobQueueDto CreateJobQueueDto(HangfireDbContext connection, string queue, bool isFetched)
        {
            var job = new JobDto
            {
                CreatedAt = DateTime.UtcNow,
                StateHistory = new []{new StateDto()}
            };

            connection.Job.InsertOne(job);

            var jobQueue = new JobQueueDto
            {
                Queue = queue,
                JobId = job.Id
            };

            if (isFetched)
            {
                jobQueue.FetchedAt = DateTime.UtcNow.AddDays(-1);
            }

            connection.JobQueue.InsertOne(jobQueue);

            return jobQueue;
        }

        private static MongoJobQueueMonitoringApi CreateMongoJobQueueMonitoringApi(HangfireDbContext connection)
        {
            return new MongoJobQueueMonitoringApi(connection);
        }

        private static void UseConnection(Action<HangfireDbContext> action)
        {
            using (var connection = ConnectionUtils.CreateConnection())
            {
                action(connection);
            }
        }
    }
#pragma warning restore 1591
}
