using System;
using System.Collections.Generic;
using System.Linq;
using Hangfire.Common;
using Hangfire.Mongo.Database;
using Hangfire.Mongo.Dto;
using Hangfire.Mongo.PersistentJobQueue;
using Hangfire.Mongo.Tests.Utils;
using Hangfire.States;
using Hangfire.Storage;
using MongoDB.Bson;
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
        private readonly Mock<IPersistentJobQueueMonitoringApi> _persistentJobQueueMonitoringApi;
        private readonly PersistentJobQueueProviderCollection _providers;

        public MongoMonitoringApiFacts()
        {
            var queue = new Mock<IPersistentJobQueue>();
            _persistentJobQueueMonitoringApi = new Mock<IPersistentJobQueueMonitoringApi>();

            var provider = new Mock<IPersistentJobQueueProvider>();
            provider.Setup(x => x.GetJobQueue(It.IsNotNull<HangfireDbContext>())).Returns(queue.Object);
            provider.Setup(x => x.GetJobQueueMonitoringApi(It.IsNotNull<HangfireDbContext>()))
                .Returns(_persistentJobQueueMonitoringApi.Object);

            _providers = new PersistentJobQueueProviderCollection(provider.Object);
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
                CreateJobInState(database, ObjectId.GenerateNewId(1), EnqueuedState.StateName);
                CreateJobInState(database, ObjectId.GenerateNewId(2), EnqueuedState.StateName);
                CreateJobInState(database, ObjectId.GenerateNewId(4), FailedState.StateName);
                CreateJobInState(database, ObjectId.GenerateNewId(5), ProcessingState.StateName);
                CreateJobInState(database, ObjectId.GenerateNewId(6), ScheduledState.StateName);
                CreateJobInState(database, ObjectId.GenerateNewId(7), ScheduledState.StateName);

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
                var result = monitoringApi.JobDetails(ObjectId.GenerateNewId().ToString());
                Assert.Null(result);
            });
        }

        [Fact, CleanDatabase]
        public void JobDetails_ReturnsResult_WhenJobExists()
        {
            UseMonitoringApi((database, monitoringApi) =>
            {
                var job1 = CreateJobInState(database, ObjectId.GenerateNewId(1), EnqueuedState.StateName);

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
                _persistentJobQueueMonitoringApi.Setup(x => x
                    .GetEnqueuedJobIds(DefaultQueue, From, PerPage))
                    .Returns(new List<string>());

                var resultList = monitoringApi.EnqueuedJobs(DefaultQueue, From, PerPage);

                Assert.Empty(resultList);
            });
        }

        [Fact, CleanDatabase]
        public void EnqueuedJobs_ReturnsSingleJob_WhenOneJobExistsThatIsNotFetched()
        {
            UseMonitoringApi((database, monitoringApi) =>
            {
                var unfetchedJob = CreateJobInState(database, ObjectId.GenerateNewId(1), EnqueuedState.StateName);

                var jobIds = new List<string> { unfetchedJob.Id.ToString() };
                _persistentJobQueueMonitoringApi.Setup(x => x
                    .GetEnqueuedJobIds(DefaultQueue, From, PerPage))
                    .Returns(jobIds);

                var resultList = monitoringApi.EnqueuedJobs(DefaultQueue, From, PerPage);

                Assert.Single(resultList);
            });
        }

        [Fact, CleanDatabase]
        public void EnqueuedJobs_ReturnsEmpty_WhenOneJobExistsThatIsFetched()
        {
            UseMonitoringApi((database, monitoringApi) =>
            {
                var fetchedJob = CreateJobInState(database, ObjectId.GenerateNewId(1), FetchedStateName);

                var jobIds = new List<string> { fetchedJob.Id.ToString() };
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
                var unfetchedJob = CreateJobInState(database, ObjectId.GenerateNewId(1), EnqueuedState.StateName);
                var unfetchedJob2 = CreateJobInState(database, ObjectId.GenerateNewId(2), EnqueuedState.StateName);
                var fetchedJob = CreateJobInState(database, ObjectId.GenerateNewId(3), FetchedStateName);

                var jobIds = new List<string>
                {
                    unfetchedJob.Id.ToString(),
                    unfetchedJob2.Id.ToString(),
                    fetchedJob.Id.ToString()
                };
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
                _persistentJobQueueMonitoringApi.Setup(x => x
                    .GetFetchedJobIds(DefaultQueue, From, PerPage))
                    .Returns(new List<string>());

                var resultList = monitoringApi.FetchedJobs(DefaultQueue, From, PerPage);

                Assert.Empty(resultList);
            });
        }

        [Fact, CleanDatabase]
        public void FetchedJobs_ReturnsSingleJob_WhenOneJobExistsThatIsFetched()
        {
            UseMonitoringApi((database, monitoringApi) =>
            {
                var fetchedJob = CreateJobInState(database, ObjectId.GenerateNewId(1), FetchedStateName);

                var jobIds = new List<string> { fetchedJob.Id.ToString() };
                _persistentJobQueueMonitoringApi.Setup(x => x
                    .GetFetchedJobIds(DefaultQueue, From, PerPage))
                    .Returns(jobIds);

                var resultList = monitoringApi.FetchedJobs(DefaultQueue, From, PerPage);

                Assert.Single(resultList);
            });
        }

        [Fact, CleanDatabase]
        public void FetchedJobs_ReturnsEmpty_WhenOneJobExistsThatIsNotFetched()
        {
            UseMonitoringApi((database, monitoringApi) =>
            {
                var unfetchedJob = CreateJobInState(database, ObjectId.GenerateNewId(1), EnqueuedState.StateName);

                var jobIds = new List<string> { unfetchedJob.Id.ToString() };
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
                var fetchedJob = CreateJobInState(database, ObjectId.GenerateNewId(1), FetchedStateName);
                var fetchedJob2 = CreateJobInState(database, ObjectId.GenerateNewId(2), FetchedStateName);
                var unfetchedJob = CreateJobInState(database, ObjectId.GenerateNewId(3), EnqueuedState.StateName);

                var jobIds = new List<string>
                {
                    fetchedJob.Id.ToString(),
                    fetchedJob2.Id.ToString(),
                    unfetchedJob.Id.ToString()
                };
                _persistentJobQueueMonitoringApi.Setup(x => x
                    .GetFetchedJobIds(DefaultQueue, From, PerPage))
                    .Returns(jobIds);

                var resultList = monitoringApi.FetchedJobs(DefaultQueue, From, PerPage);

                Assert.Equal(2, resultList.Count);
            });
        }

        [Fact, CleanDatabase]
        public void ProcessingJobs_ReturnsProcessingJobsOnly_WhenMultipleJobsExistsInProcessingSucceededAndEnqueuedState()
        {
            UseMonitoringApi((database, monitoringApi) =>
            {
                var processingJob = CreateJobInState(database, ObjectId.GenerateNewId(1), ProcessingState.StateName);

                var succeededJob = CreateJobInState(database, ObjectId.GenerateNewId(2), SucceededState.StateName, jobDto =>
                {
                    var processingState = new StateDto()
                    {
                        Name = ProcessingState.StateName,
                        Reason = null,
                        CreatedAt = DateTime.UtcNow,
                        Data = new Dictionary<string, string>
                        {
                            ["ServerId"] = Guid.NewGuid().ToString(),
                            ["StartedAt"] =
                            JobHelper.SerializeDateTime(DateTime.UtcNow.Subtract(TimeSpan.FromMilliseconds(500)))
                        }
                    };
                    var succeededState = jobDto.StateHistory[0];
                    jobDto.StateHistory = new[] { processingState, succeededState };
                    return jobDto;
                });

                var enqueuedJob = CreateJobInState(database, ObjectId.GenerateNewId(3), EnqueuedState.StateName);

                var jobIds = new List<string>
                {
                    processingJob.Id.ToString(),
                    succeededJob.Id.ToString(),
                    enqueuedJob.Id.ToString()
                };
                _persistentJobQueueMonitoringApi.Setup(x => x
                        .GetFetchedJobIds(DefaultQueue, From, PerPage))
                    .Returns(jobIds);

                var resultList = monitoringApi.ProcessingJobs(From, PerPage);

                Assert.Single(resultList);
            });
        }

        [Fact, CleanDatabase]
        public void FailedJobs_ReturnsFailedJobs_InDescendingOrder()
        {
            UseMonitoringApi((database, monitoringApi) =>
            {
                var failedJob0 = CreateJobInState(database, ObjectId.GenerateNewId(1), FailedState.StateName);
                var failedJob1 = CreateJobInState(database, ObjectId.GenerateNewId(2), FailedState.StateName);
                var failedJob2 = CreateJobInState(database, ObjectId.GenerateNewId(3), FailedState.StateName);


                var jobIds = new List<string>
                {
                    failedJob0.Id.ToString(),
                    failedJob1.Id.ToString(),
                    failedJob2.Id.ToString()
                };
                _persistentJobQueueMonitoringApi.Setup(x => x
                        .GetFetchedJobIds(DefaultQueue, From, PerPage))
                    .Returns(jobIds);

                var resultList = monitoringApi.FailedJobs(From, PerPage);

                Assert.Equal(failedJob0.Id.ToString(), resultList[2].Key);
                Assert.Equal(failedJob1.Id.ToString(), resultList[1].Key);
                Assert.Equal(failedJob2.Id.ToString(), resultList[0].Key);
            });
        }
        
        [Fact, CleanDatabase]
        public void SucceededByDatesCount_ReturnsSuccededJobs_ForLastWeek()
        {
            UseMonitoringApi((database, monitoringApi) =>
            {
                var date = DateTime.UtcNow.Date;
                var succededCount = 10L;
                
                database.JobGraph.OfType<CounterDto>().InsertOne(new CounterDto
                {
                    Id = ObjectId.GenerateNewId(),
                    // this might fail if we test during date change... seems unlikely
                    // TODO, wrap Datetime in a mock friendly wrapper
                    Key = $"stats:succeeded:{date:yyyy-MM-dd}", 
                    Value = succededCount
                });
                
           var results = monitoringApi.SucceededByDatesCount();
                
                Assert.Equal(succededCount, results[date]);
                Assert.Equal(8, results.Count);
            });
        }
        
        [Fact, CleanDatabase]
        public void HourlySucceededJobs_ReturnsSuccededJobs_ForLast24Hours()
        {
            UseMonitoringApi((database, monitoringApi) =>
            {
                var now = DateTime.UtcNow;
                
                var succeededCount = 10L;
                database.JobGraph.InsertOne(new CounterDto
                {
                    Id = ObjectId.GenerateNewId(),
                    // this might fail if we test during hour change... still unlikely
                    // TODO, wrap Datetime in a mock friendly wrapper
                    Key = $"stats:succeeded:{now:yyyy-MM-dd-HH}", 
                    Value = succeededCount
                });
                
                var results = monitoringApi.HourlySucceededJobs();
                
                Assert.Equal(succeededCount, results.First(kv => kv.Key.Hour.Equals(now.Hour)).Value);
                Assert.Equal(24, results.Count);

            });
        }
        
        [Fact, CleanDatabase]
        public void FailedByDatesCount_ReturnsFailedJobs_ForLastWeek()
        {
            UseMonitoringApi((database, monitoringApi) =>
            {
                var date = DateTime.UtcNow.Date;
                var failedCount = 10L;
                
                database.JobGraph.OfType<CounterDto>().InsertOne(new CounterDto
                {
                    Id = ObjectId.GenerateNewId(),
                    // this might fail if we test during date change... seems unlikely
                    Key = $"stats:failed:{date:yyyy-MM-dd}", 
                    Value = failedCount
                });
                
                var results = monitoringApi.FailedByDatesCount();
                
                Assert.Equal(failedCount, results[date]);
                Assert.Equal(8, results.Count);

            });
        }
        
        [Fact, CleanDatabase]
        public void HourlyFailedJobs_ReturnsFailedJobs_ForLast24Hours()
        {
            UseMonitoringApi((database, monitoringApi) =>
            {
                var now = DateTime.UtcNow;
                var failedCount = 10L;
              
                database.JobGraph.OfType<CounterDto>().InsertOne(new CounterDto
                {
                    Id = ObjectId.GenerateNewId(),
                    // this might fail if we test during hour change... still unlikely
                    // TODO, wrap Datetime in a mock friendly wrapper
                    Key = $"stats:failed:{now:yyyy-MM-dd-HH}", 
                    Value = failedCount
                });
               
                var results = monitoringApi.HourlyFailedJobs();
                
                Assert.Equal(failedCount, results.First(kv => kv.Key.Hour.Equals(now.Hour)).Value);
                Assert.Equal(24, results.Count);

            });
        }

        private void UseMonitoringApi(Action<HangfireDbContext, MongoMonitoringApi> action)
        {
            using (var database = ConnectionUtils.CreateConnection())
            {
                var connection = new MongoMonitoringApi(database, _providers);
                action(database, connection);
            }
        }

        private JobDto CreateJobInState(HangfireDbContext database, ObjectId jobId, string stateName, Func<JobDto, JobDto> visitor = null)
        {
            var job = Job.FromExpression(() => HangfireTestJobs.SampleMethod("wrong"));

            Dictionary<string, string> stateData;
            if (stateName == EnqueuedState.StateName)
            {
                stateData = new Dictionary<string, string> { ["EnqueuedAt"] = $"{DateTime.UtcNow:o}" };
            }
            else if (stateName == ProcessingState.StateName)
            {
                stateData = new Dictionary<string, string>
                {
                    ["ServerId"] = Guid.NewGuid().ToString(),
                    ["StartedAt"] = JobHelper.SerializeDateTime(DateTime.UtcNow.Subtract(TimeSpan.FromMilliseconds(500)))
                };
            }
            else if (stateName == FailedState.StateName)
            {
                stateData = new Dictionary<string, string>
                {
                    ["ExceptionDetails"] = "Test_ExceptionDetails",
                    ["ExceptionMessage"] = "Test_ExceptionMessage",
                    ["ExceptionType"] = "Test_ExceptionType",
                    ["FailedAt"] = JobHelper.SerializeDateTime(DateTime.UtcNow.Subtract(TimeSpan.FromMilliseconds(10)))
                };
            }
            else
            {
                stateData = new Dictionary<string, string>();
            }

            var jobState = new StateDto()
            {
                Name = stateName,
                Reason = null,
                CreatedAt = DateTime.UtcNow,
                Data = stateData
            };

            var jobDto = new JobDto
            {
                Id = jobId,
                InvocationData = JobHelper.ToJson(InvocationData.Serialize(job)),
                Arguments = "[\"\\\"Arguments\\\"\"]",
                StateName = stateName,
                CreatedAt = DateTime.UtcNow,
                StateHistory = new[] { jobState }
            };
            if (visitor != null)
            {
                jobDto = visitor(jobDto);
            }
            database.JobGraph.InsertOne(jobDto);

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

            database.JobGraph.InsertOne(jobQueueDto);

            return jobDto;
        }
    }
#pragma warning restore 1591
}
