using System;
using System.Collections.Generic;
using System.Linq;
using Hangfire.Common;
using Hangfire.Mongo.Database;
using Hangfire.Mongo.Dto;
using Hangfire.Mongo.Tests.Utils;
using Hangfire.States;
using Hangfire.Storage;
using MongoDB.Bson;
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
        private readonly HangfireDbContext _database;
        private readonly MongoMonitoringApi _monitoringApi;

        public MongoMonitoringApiFacts(MongoIntegrationTestFixture fixture)
        {
            fixture.CleanDatabase();
            _database = fixture.CreateDbContext();
            _monitoringApi = new MongoMonitoringApi(_database);
        }

        [Fact]
        public void GetStatistics_ReturnsZero_WhenNoJobsExist()
        {
            var result = _monitoringApi.GetStatistics();
            Assert.Equal(0, result.Enqueued);
            Assert.Equal(0, result.Failed);
            Assert.Equal(0, result.Processing);
            Assert.Equal(0, result.Scheduled);
            Assert.Equal(0, result.Awaiting);
            Assert.Equal(0, result.Deleted);
        }

        [Fact]
        public void GetStatistics_ReturnsExpectedCounts_WhenJobsExist()
        {
            CreateJobInState(_database, ObjectId.GenerateNewId(1), EnqueuedState.StateName);
            CreateJobInState(_database, ObjectId.GenerateNewId(2), EnqueuedState.StateName);
            CreateJobInState(_database, ObjectId.GenerateNewId(4), FailedState.StateName);
            CreateJobInState(_database, ObjectId.GenerateNewId(5), ProcessingState.StateName);
            CreateJobInState(_database, ObjectId.GenerateNewId(6), ScheduledState.StateName);
            CreateJobInState(_database, ObjectId.GenerateNewId(7), ScheduledState.StateName);
            CreateJobInState(_database, ObjectId.GenerateNewId(10), AwaitingState.StateName);
            CreateJobInState(_database, ObjectId.GenerateNewId(11), AwaitingState.StateName);
            CreateJobInState(_database, ObjectId.GenerateNewId(12), AwaitingState.StateName);
            CreateJobInState(_database, ObjectId.GenerateNewId(13), AwaitingState.StateName);

            var result = _monitoringApi.GetStatistics();
            Assert.Equal(2, result.Enqueued);
            Assert.Equal(1, result.Failed);
            Assert.Equal(1, result.Processing);
            Assert.Equal(2, result.Scheduled);
            Assert.Equal(4, result.Awaiting);
        }

        [Fact]
        public void GetStatistics_RecurringJob_CountsSets()
        {
            const string setJson = @"
                            {
                                '_id':'5be18a91139c3a01128c8066',
                                'Key':'recurring-jobs:HomeController.PrintToDebug',
                                'Value':'HomeController.PrintToDebug',
                                '_t':['BaseJobDto','ExpiringJobDto','KeyJobDto','SetDto'],
                                'Score':0,
                                'ExpireAt':null
                            }";
            _database
                .Database
                .GetCollection<BsonDocument>(_database.JobGraph.CollectionNamespace.CollectionName)
                .InsertOne(BsonDocument.Parse(setJson));

            var result = _monitoringApi.GetStatistics();
            Assert.Equal(1, result.Recurring);
        }

        [Fact]
        public void JobDetails_ReturnsNull_WhenThereIsNoSuchJob()
        {
            var result = _monitoringApi.JobDetails(ObjectId.GenerateNewId().ToString());
            Assert.Null(result);
        }

        [Fact]
        public void JobDetails_ReturnsResult_WhenJobExists()
        {
            var job1 = CreateJobInState(_database, ObjectId.GenerateNewId(1), EnqueuedState.StateName);

            var result = _monitoringApi.JobDetails(job1.Id.ToString());

            Assert.NotNull(result);
            Assert.NotNull(result.Job);
            Assert.Equal("Arguments", result.Job.Args[0]);
            Assert.True(DateTime.UtcNow.AddMinutes(-1) < result.CreatedAt);
            Assert.True(result.CreatedAt < DateTime.UtcNow.AddMinutes(1));
        }

        [Fact]
        public void EnqueuedJobs_ReturnsEmpty_WhenThereIsNoJobs()
        {
            var resultList = _monitoringApi.EnqueuedJobs(DefaultQueue, From, PerPage);

            Assert.Empty(resultList);
        }

        [Fact]
        public void EnqueuedJobs_ReturnsSingleJob_WhenOneJobExistsThatIsNotFetched()
        {
            CreateJobInState(_database, ObjectId.GenerateNewId(1), EnqueuedState.StateName);
            var resultList = _monitoringApi.EnqueuedJobs(DefaultQueue, From, PerPage);

            Assert.Single(resultList);
        }

        [Fact]
        public void EnqueuedJobs_ReturnsEmpty_WhenOneJobExistsThatIsFetched()
        {
            CreateJobInState(_database, ObjectId.GenerateNewId(1), FetchedStateName);
            var resultList = _monitoringApi.EnqueuedJobs(DefaultQueue, From, PerPage);

            Assert.Empty(resultList);
        }

        [Fact]
        public void EnqueuedJobs_ReturnsUnfetchedJobsOnly_WhenMultipleJobsExistsInFetchedAndUnfetchedStates()
        {
            CreateJobInState(_database, ObjectId.GenerateNewId(1), EnqueuedState.StateName);
            CreateJobInState(_database, ObjectId.GenerateNewId(2), EnqueuedState.StateName);
            CreateJobInState(_database, ObjectId.GenerateNewId(3), FetchedStateName);

            var resultList = _monitoringApi.EnqueuedJobs(DefaultQueue, From, PerPage);

            Assert.Equal(2, resultList.Count);
        }

        [Fact]
        public void FetchedJobs_ReturnsEmpty_WhenThereIsNoJobs()
        {
            var resultList = _monitoringApi.FetchedJobs(DefaultQueue, From, PerPage);

            Assert.Empty(resultList);
        }

        [Fact]
        public void FetchedJobs_ReturnsSingleJob_WhenOneJobExistsThatIsFetched()
        {
            CreateJobInState(_database, ObjectId.GenerateNewId(1), FetchedStateName);
            var resultList = _monitoringApi.FetchedJobs(DefaultQueue, From, PerPage);

            Assert.Single(resultList);
        }

        [Fact]
        public void FetchedJobs_ReturnsEmpty_WhenOneJobExistsThatIsNotFetched()
        {
            CreateJobInState(_database, ObjectId.GenerateNewId(1), EnqueuedState.StateName);
            var resultList = _monitoringApi.FetchedJobs(DefaultQueue, From, PerPage);

            Assert.Empty(resultList);
        }

        [Fact]
        public void FetchedJobs_ReturnsFetchedJobsOnly_WhenMultipleJobsExistsInFetchedAndUnfetchedStates()
        {
            CreateJobInState(_database, ObjectId.GenerateNewId(1), FetchedStateName);
            CreateJobInState(_database, ObjectId.GenerateNewId(2), FetchedStateName);
            CreateJobInState(_database, ObjectId.GenerateNewId(3), EnqueuedState.StateName);

            var resultList = _monitoringApi.FetchedJobs(DefaultQueue, From, PerPage);

            Assert.Equal(2, resultList.Count);
        }
        
        [Fact]
        public void DeletedJobs_MultipleJobsExistsInDeletedAndNonDeletedStates_ReturnsDeletedJobsOnly()
        {
            CreateJobInState(_database, ObjectId.GenerateNewId(1), FetchedStateName);
            CreateJobInState(_database, ObjectId.GenerateNewId(2), FetchedStateName);
            CreateJobInState(_database, ObjectId.GenerateNewId(3), DeletedState.StateName);

            var resultList = _monitoringApi.DeletedJobs(From, PerPage);

            Assert.Single(resultList);
        }
        
        [Fact]
        public void AwaitingJobs_MultipleJobsExistsInAwaitingAndNonAwaitingStates_ReturnsAwaitingJobsOnly()
        {
            CreateJobInState(_database, ObjectId.GenerateNewId(1), FetchedStateName);
            CreateJobInState(_database, ObjectId.GenerateNewId(2), FetchedStateName);
            CreateJobInState(_database, ObjectId.GenerateNewId(3), AwaitingState.StateName);

            var resultList = _monitoringApi.AwaitingJobs(From, PerPage);

            Assert.Single(resultList);
        }

        [Fact]
        public void ProcessingJobs_ReturnsProcessingJobsOnly_WhenMultipleJobsExistsInProcessingSucceededAndEnqueuedState()
        {
            CreateJobInState(_database, ObjectId.GenerateNewId(1), ProcessingState.StateName);

            CreateJobInState(_database, ObjectId.GenerateNewId(2), SucceededState.StateName, jobDto =>
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

            CreateJobInState(_database, ObjectId.GenerateNewId(3), EnqueuedState.StateName);

            var resultList = _monitoringApi.ProcessingJobs(From, PerPage);

            Assert.Single(resultList);
        }



        [Fact]
        public void ProcessingJobs_ReturnsLatestStateHistory_WhenJobRequeued()
        {
            var oldServerId = "oldserverid";
            var newServerId = "newserverid";
            var oldStartTime = DateTime.UtcNow.AddMinutes(-30);
            var newStartTime = DateTime.UtcNow;

            CreateJobInState(_database, ObjectId.GenerateNewId(2), ProcessingState.StateName, jobDto =>
            {
                var firstProcessingState = new StateDto()
                {
                    Name = ProcessingState.StateName,
                    Reason = null,
                    CreatedAt = oldStartTime,
                    Data = new Dictionary<string, string>
                    {
                        ["ServerId"] = oldServerId,
                        ["StartedAt"] = JobHelper.SerializeDateTime(oldStartTime)
                    }
                };
                var latestProcessingState = jobDto.StateHistory[0];
                latestProcessingState.CreatedAt = newStartTime;
                latestProcessingState.Data["ServerId"] = newServerId;
                latestProcessingState.Data["StartedAt"] = JobHelper.SerializeDateTime(newStartTime);
                jobDto.StateHistory = new[] { firstProcessingState, latestProcessingState };
                return jobDto;
            });

            var resultList = _monitoringApi.ProcessingJobs(From, PerPage);

            Assert.Single(resultList);
            Assert.Equal(newServerId, resultList[0].Value.ServerId);
            Assert.Equal(newStartTime, resultList[0].Value.StartedAt.Value, TimeSpan.FromMilliseconds(10));
        }

        [Fact]
        public void FailedJobs_ReturnsFailedJobs_InDescendingOrder()
        {
            var failedJob0 = CreateJobInState(_database, ObjectId.GenerateNewId(1), FailedState.StateName);
            var failedJob1 = CreateJobInState(_database, ObjectId.GenerateNewId(2), FailedState.StateName);
            var failedJob2 = CreateJobInState(_database, ObjectId.GenerateNewId(3), FailedState.StateName);

            var resultList = _monitoringApi.FailedJobs(From, PerPage);

            Assert.Equal(failedJob0.Id.ToString(), resultList[2].Key);
            Assert.Equal(failedJob1.Id.ToString(), resultList[1].Key);
            Assert.Equal(failedJob2.Id.ToString(), resultList[0].Key);
        }

        [Fact]
        public void SucceededByDatesCount_ReturnsSuccededJobs_ForLastWeek()
        {
            var date = DateTime.UtcNow.Date;
            var succededCount = 10L;

            _database.JobGraph.InsertOne(new CounterDto
            {
                Id = ObjectId.GenerateNewId(),
                // this might fail if we test during date change... seems unlikely
                // TODO, wrap Datetime in a mock friendly wrapper
                Key = $"stats:succeeded:{date:yyyy-MM-dd}",
                Value = succededCount
            }.Serialize());

            var results = _monitoringApi.SucceededByDatesCount();

            Assert.Equal(succededCount, results[date]);
            Assert.Equal(8, results.Count);
        }

        [Fact]
        public void HourlySucceededJobs_ReturnsSuccededJobs_ForLast24Hours()
        {
            var now = DateTime.UtcNow;

            var succeededCount = 10L;
            _database.JobGraph.InsertOne(new CounterDto
            {
                Id = ObjectId.GenerateNewId(),
                // this might fail if we test during hour change... still unlikely
                // TODO, wrap Datetime in a mock friendly wrapper
                Key = $"stats:succeeded:{now:yyyy-MM-dd-HH}",
                Value = succeededCount
            }.Serialize());

            var results = _monitoringApi.HourlySucceededJobs();

            Assert.Equal(succeededCount, results.First(kv => kv.Key.Hour.Equals(now.Hour)).Value);
            Assert.Equal(24, results.Count);
        }

        [Fact]
        public void FailedByDatesCount_ReturnsFailedJobs_ForLastWeek()
        {
            var date = DateTime.UtcNow.Date;
            var failedCount = 10L;

            _database.JobGraph.InsertOne(new CounterDto
            {
                Id = ObjectId.GenerateNewId(),
                // this might fail if we test during date change... seems unlikely
                Key = $"stats:failed:{date:yyyy-MM-dd}",
                Value = failedCount
            }.Serialize());

            var results = _monitoringApi.FailedByDatesCount();

            Assert.Equal(failedCount, results[date]);
            Assert.Equal(8, results.Count);
        }

        [Fact]
        public void HourlyFailedJobs_ReturnsFailedJobs_ForLast24Hours()
        {
            var now = DateTime.UtcNow;
            var failedCount = 10L;

            _database.JobGraph.InsertOne(new CounterDto
            {
                Id = ObjectId.GenerateNewId(),
                // this might fail if we test during hour change... still unlikely
                // TODO, wrap Datetime in a mock friendly wrapper
                Key = $"stats:failed:{now:yyyy-MM-dd-HH}",
                Value = failedCount
            }.Serialize());

            var results = _monitoringApi.HourlyFailedJobs();

            Assert.Equal(failedCount, results.First(kv => kv.Key.Hour.Equals(now.Hour)).Value);
            Assert.Equal(24, results.Count);
        }

        [Fact]
        public void GetQueues_Queues_ReturnedDistinct()
        {
            // ARRANGE
            CreateJobInState(_database, ObjectId.GenerateNewId(1), EnqueuedState.StateName, job =>
            {
                job.Queue = "queue_1";
                return job;
            });

            CreateJobInState(_database, ObjectId.GenerateNewId(2), EnqueuedState.StateName, job =>
            {
                job.Queue = "queue_2";
                return job;
            });

            CreateJobInState(_database, ObjectId.GenerateNewId(3), EnqueuedState.StateName, job =>
            {
                job.Queue = "queue_2";
                return job;
            });
            
            // ACT
            var results = _monitoringApi.GetQueues();
            
            // ASSERT
            Assert.Equal(new[] { "queue_1", "queue_2" }, results);
        }

        [Fact]
        public void GetQueues_NullQueue_Excluded()
        {
            // ARRANGE
            CreateJobInState(_database, ObjectId.GenerateNewId(1), EnqueuedState.StateName, job =>
            {
                job.Queue = "queue_1";
                return job;
            });

            CreateJobInState(_database, ObjectId.GenerateNewId(2), EnqueuedState.StateName, job =>
            {
                job.Queue = "queue_2";
                return job;
            });

            CreateJobInState(_database, ObjectId.GenerateNewId(3), EnqueuedState.StateName, job =>
            {
                job.Queue = null;
                return job;
            });
            
            // ACT
            var results = _monitoringApi.GetQueues();
            
            // ASSERT
            Assert.Equal(new[] { "queue_1", "queue_2" }, results);
        }

        [Fact]
        public void GetStatistics_ReturnsQueueCount()
        {
            CreateJobInState(_database, ObjectId.GenerateNewId(1), EnqueuedState.StateName, job =>
            {
                job.Queue = "queue_1";
                return job;
            });

            CreateJobInState(_database, ObjectId.GenerateNewId(2), EnqueuedState.StateName, job =>
            {
                job.Queue = "queue_2";
                return job;
            });

            CreateJobInState(_database, ObjectId.GenerateNewId(3), EnqueuedState.StateName, job =>
            {
                job.Queue = "queue_2";
                return job;
            });

            var statistics = _monitoringApi.GetStatistics();
            
            Assert.Equal(2, statistics.Queues);
        }

        private JobDto CreateJobInState(HangfireDbContext dbContext, ObjectId jobId, string stateName, Func<JobDto, JobDto> visitor = null)
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
                StateHistory = new[] { jobState },
                Queue = DefaultQueue,
                FetchedAt = null
            };
            if (visitor != null)
            {
                jobDto = visitor(jobDto);
            }
            if (stateName == FetchedStateName)
            {
                jobDto.FetchedAt = DateTime.UtcNow;
            }

            dbContext.JobGraph.InsertOne(jobDto.Serialize());

            return jobDto;
        }
    }
#pragma warning restore 1591
}
