using System;
using System.Collections.Generic;
using System.Diagnostics;
using Hangfire.Common;
using Hangfire.Mongo.Database;
using Hangfire.Mongo.Dto;
using Hangfire.Mongo.PersistentJobQueue;
using Hangfire.Mongo.Tests.Utils;
using Hangfire.States;
using Hangfire.Storage;
using Moq;
using Xunit;

namespace Hangfire.Mongo.Tests
{
#pragma warning disable 1591
    [Collection("Database")]
    public class MongoMonitoringApiFacts
    {
        private const string DefaultQueue = "default";
        private const string FetchedStateName = "Fetched";
        private const int From = 0;
        private const int PerPage = 5;
        private readonly Mock<IPersistentJobQueue> _queue;
        private readonly Mock<IPersistentJobQueueProvider> _provider;
        private readonly Mock<IPersistentJobQueueMonitoringApi> _persistentJobQueueMonitoringApi;
        private readonly PersistentJobQueueProviderCollection _providers;

        public MongoMonitoringApiFacts()
        {
            _queue = new Mock<IPersistentJobQueue>();
            _persistentJobQueueMonitoringApi = new Mock<IPersistentJobQueueMonitoringApi>();

            _provider = new Mock<IPersistentJobQueueProvider>();
            _provider.Setup(x => x.GetJobQueue(It.IsNotNull<HangfireDbContext>())).Returns(_queue.Object);
            _provider.Setup(x => x.GetJobQueueMonitoringApi(It.IsNotNull<HangfireDbContext>()))
                .Returns(_persistentJobQueueMonitoringApi.Object);

            _providers = new PersistentJobQueueProviderCollection(_provider.Object);
        }

        [Fact, CleanDatabase]
        public void GetStatistics_ReturnsZero_WhenNoJobsExist()
        {
            UseMonitoringApi((database, monitoringApi) =>
            {
                var result = monitoringApi.GetStatistics();
                Assert.Equal(0, result.Enqueued);
                Assert.Equal(0, result.Failed);
                Assert.Equal(0, result.Processing);
                Assert.Equal(0, result.Scheduled);
            });
        }

        [Fact, CleanDatabase]
        public void GetStatistics_ReturnsExpectedCounts_WhenJobsExist()
        {
            UseMonitoringApi((database, monitoringApi) =>
            {
                CreateJobInState(database, 1.ToString(), EnqueuedState.StateName);
                CreateJobInState(database, 2.ToString(), EnqueuedState.StateName);
                CreateJobInState(database, 4.ToString(), FailedState.StateName);
                CreateJobInState(database, 5.ToString(), ProcessingState.StateName);
                CreateJobInState(database, 6.ToString(), ScheduledState.StateName);
                CreateJobInState(database, 7.ToString(), ScheduledState.StateName);

                var result = monitoringApi.GetStatistics();
                Assert.Equal(2, result.Enqueued);
                Assert.Equal(1, result.Failed);
                Assert.Equal(1, result.Processing);
                Assert.Equal(2, result.Scheduled);
            });
        }

        [Fact, CleanDatabase]
        public void JobDetails_ReturnsNull_WhenThereIsNoSuchJob()
        {
            UseMonitoringApi((database, monitoringApi) =>
            {
                var result = monitoringApi.JobDetails("547527");
                Assert.Null(result);
            });
        }

        [Fact, CleanDatabase]
        public void JobDetails_ReturnsResult_WhenJobExists()
        {
            UseMonitoringApi((database, monitoringApi) =>
            {
                var job1 = CreateJobInState(database, 1.ToString(), EnqueuedState.StateName);

                var result = monitoringApi.JobDetails(job1.Id.ToString());

                Assert.NotNull(result);
                Assert.NotNull(result.Job);
                Assert.Equal("Arguments", result.Job.Args[0]);
                Assert.True(DateTime.UtcNow.AddMinutes(-1) < result.CreatedAt);
                Assert.True(result.CreatedAt < DateTime.UtcNow.AddMinutes(1));
            });
        }

        [Fact, CleanDatabase]
        public void EnqueuedJobs_ReturnsEmpty_WhenThereIsNoJobs()
        {
            UseMonitoringApi((database, monitoringApi) =>
            {
                var jobIds = new List<string>();

                _persistentJobQueueMonitoringApi.Setup(x => x
                    .GetEnqueuedJobIds(DefaultQueue, From, PerPage))
                    .Returns(jobIds);

                var resultList = monitoringApi.EnqueuedJobs(DefaultQueue, From, PerPage);

                Assert.Empty(resultList);
            });
        }

        [Fact, CleanDatabase]
        public void EnqueuedJobs_ReturnsSingleJob_WhenOneJobExistsThatIsNotFetched()
        {
            UseMonitoringApi((database, monitoringApi) =>
            {
                var unfetchedJob = CreateJobInState(database, 1.ToString(), EnqueuedState.StateName);

                var jobIds = new List<string> { unfetchedJob.Id };
                _persistentJobQueueMonitoringApi.Setup(x => x
                    .GetEnqueuedJobIds(DefaultQueue, From, PerPage))
                    .Returns(jobIds);

                var resultList = monitoringApi.EnqueuedJobs(DefaultQueue, From, PerPage);

                Assert.Equal(1, resultList.Count);
            });
        }

        [Fact, CleanDatabase]
        public void EnqueuedJobs_ReturnsEmpty_WhenOneJobExistsThatIsFetched()
        {
            UseMonitoringApi((database, monitoringApi) =>
            {
                var fetchedJob = CreateJobInState(database, 1.ToString(), FetchedStateName);

                var jobIds = new List<string> { fetchedJob.Id };
                _persistentJobQueueMonitoringApi.Setup(x => x
                    .GetEnqueuedJobIds(DefaultQueue, From, PerPage))
                    .Returns(jobIds);

                var resultList = monitoringApi.EnqueuedJobs(DefaultQueue, From, PerPage);

                Assert.Empty(resultList);
            });
        }

        [Fact, CleanDatabase]
        public void EnqueuedJobs_ReturnsUnfetchedJobsOnly_WhenMultipleJobsExistsInFetchedAndUnfetchedStates()
        {
            UseMonitoringApi((database, monitoringApi) =>
            {
                var unfetchedJob = CreateJobInState(database, 1.ToString(), EnqueuedState.StateName);
                var unfetchedJob2 = CreateJobInState(database, 2.ToString(), EnqueuedState.StateName);
                var fetchedJob = CreateJobInState(database, 3.ToString(), FetchedStateName);

                var jobIds = new List<string> { unfetchedJob.Id, unfetchedJob2.Id, fetchedJob.Id };
                _persistentJobQueueMonitoringApi.Setup(x => x
                    .GetEnqueuedJobIds(DefaultQueue, From, PerPage))
                    .Returns(jobIds);

                var resultList = monitoringApi.EnqueuedJobs(DefaultQueue, From, PerPage);

                Assert.Equal(2, resultList.Count);
            });
        }

        [Fact, CleanDatabase]
        public void FetchedJobs_ReturnsEmpty_WhenThereIsNoJobs()
        {
            UseMonitoringApi((database, monitoringApi) =>
            {
                var jobIds = new List<string>();

                _persistentJobQueueMonitoringApi.Setup(x => x
                    .GetFetchedJobIds(DefaultQueue, From, PerPage))
                    .Returns(jobIds);

                var resultList = monitoringApi.FetchedJobs(DefaultQueue, From, PerPage);

                Assert.Empty(resultList);
            });
        }

        [Fact, CleanDatabase]
        public void FetchedJobs_ReturnsSingleJob_WhenOneJobExistsThatIsFetched()
        {
            UseMonitoringApi((database, monitoringApi) =>
            {
                var fetchedJob = CreateJobInState(database, 1.ToString(), FetchedStateName);

                var jobIds = new List<string> { fetchedJob.Id };
                _persistentJobQueueMonitoringApi.Setup(x => x
                    .GetFetchedJobIds(DefaultQueue, From, PerPage))
                    .Returns(jobIds);

                var resultList = monitoringApi.FetchedJobs(DefaultQueue, From, PerPage);

                Assert.Equal(1, resultList.Count);
            });
        }

        [Fact, CleanDatabase]
        public void FetchedJobs_ReturnsEmpty_WhenOneJobExistsThatIsNotFetched()
        {
            UseMonitoringApi((database, monitoringApi) =>
            {
                var unfetchedJob = CreateJobInState(database, 1.ToString(), EnqueuedState.StateName);

                var jobIds = new List<string> { unfetchedJob.Id };
                _persistentJobQueueMonitoringApi.Setup(x => x
                    .GetFetchedJobIds(DefaultQueue, From, PerPage))
                    .Returns(jobIds);

                var resultList = monitoringApi.FetchedJobs(DefaultQueue, From, PerPage);

                Assert.Empty(resultList);
            });
        }

        [Fact, CleanDatabase]
        public void FetchedJobs_ReturnsFetchedJobsOnly_WhenMultipleJobsExistsInFetchedAndUnfetchedStates()
        {
            UseMonitoringApi((database, monitoringApi) =>
            {
                var fetchedJob = CreateJobInState(database, 1.ToString(), FetchedStateName);
                var fetchedJob2 = CreateJobInState(database, 2.ToString(), FetchedStateName);
                var unfetchedJob = CreateJobInState(database, 3.ToString(), EnqueuedState.StateName);

                var jobIds = new List<string> { fetchedJob.Id, fetchedJob2.Id, unfetchedJob.Id };
                _persistentJobQueueMonitoringApi.Setup(x => x
                    .GetFetchedJobIds(DefaultQueue, From, PerPage))
                    .Returns(jobIds);

                var resultList = monitoringApi.FetchedJobs(DefaultQueue, From, PerPage);

                Assert.Equal(2, resultList.Count);
            });
        }

        public static void SampleMethod(string arg)
        {
            Debug.WriteLine(arg);
        }

        private void UseMonitoringApi(Action<HangfireDbContext, MongoMonitoringApi> action)
        {
            using (var database = ConnectionUtils.CreateConnection())
            {
                var connection = new MongoMonitoringApi(database, _providers);
                action(database, connection);
            }
        }

        private JobDto CreateJobInState(HangfireDbContext database, string jobId, string stateName)
        {
            var job = Job.FromExpression(() => SampleMethod("wrong"));

	        var jobState = new StateDto()
	        {
		        CreatedAt = DateTime.UtcNow,
		        Data =
			        stateName == EnqueuedState.StateName
				        ? new Dictionary<string, string> {["EnqueuedAt"] = $"{DateTime.UtcNow:o}"}
				        : new Dictionary<string, string>()
	        };

            var jobDto = new JobDto
            {
                Id = jobId,
                InvocationData = JobHelper.ToJson(InvocationData.Serialize(job)),
                Arguments = "['Arguments']",
                StateName = stateName,
                CreatedAt = DateTime.UtcNow,
                StateHistory = new []{jobState}
            };

            database.Job.InsertOne(jobDto);

            var jobQueueDto = new JobQueueDto
            {
                FetchedAt = null,
                JobId = jobId,
                Queue = DefaultQueue
            };

            if (stateName == FetchedStateName)
            {
                jobQueueDto.FetchedAt = DateTime.UtcNow;
            }

            database.JobQueue.InsertOne(jobQueueDto);

            return jobDto;
        }
    }
#pragma warning restore 1591
}
