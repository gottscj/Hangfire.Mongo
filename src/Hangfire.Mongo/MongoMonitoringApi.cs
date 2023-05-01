using System;
using System.Collections.Generic;
using System.ComponentModel.Design;
using System.Linq;
using System.Threading.Tasks;
using Hangfire.Common;
using Hangfire.Mongo.Database;
using Hangfire.Mongo.Dto;
using Hangfire.States;
using Hangfire.Storage;
using Hangfire.Storage.Monitoring;
using MongoDB.Bson;
using MongoDB.Driver;
using ServerDto = Hangfire.Storage.Monitoring.ServerDto;

namespace Hangfire.Mongo
{
#pragma warning disable 1591
    public class MongoMonitoringApi : JobStorageMonitor
    {
        private readonly HangfireDbContext _dbContext;

        public MongoMonitoringApi(HangfireDbContext dbContext)
        {
            _dbContext = dbContext;
        }

        public override IList<QueueWithTopEnqueuedJobsDto> Queues()
        {
            var queues = GetQueues();

            var result = new List<QueueWithTopEnqueuedJobsDto>(queues.Count);

            foreach (var queue in queues)
            {
                var enqueuedJobIds = GetEnqueuedJobIds(queue, 0, 5);
                var counters = GetEnqueuedAndFetchedCount(queue);

                result.Add(new QueueWithTopEnqueuedJobsDto
                {
                    Name = queue,
                    Length = counters.EnqueuedCount ?? 0,
                    Fetched = counters.FetchedCount,
                    FirstJobs = EnqueuedJobs(enqueuedJobIds)
                });
            }

            return result;
        }

        public override IList<ServerDto> Servers()
        {
            var servers = _dbContext.Server.AsQueryable().ToList().Select(b => new Dto.ServerDto(b));

            var result = new List<ServerDto>();

            foreach (var server in servers)
            {
                result.Add(new ServerDto
                {
                    Name = server.Id,
                    Heartbeat = server.LastHeartbeat,
                    Queues = server.Queues,
                    StartedAt = server.StartedAt ?? DateTime.MinValue,
                    WorkersCount = server.WorkerCount
                });
            }

            return result;
        }

        public override JobDetailsDto JobDetails(string jobId)
        {
            var id = ObjectId.Parse(jobId);
            var filter = new BsonDocument
            {
                ["_id"] = id,
                ["_t"] = nameof(JobDto)
            };
            var jobDoc = _dbContext.JobGraph.Find(filter).FirstOrDefault();

            if (jobDoc == null)
            {
                return null;
            }
            var job = new JobDto(jobDoc);
            var history = job.StateHistory.Select(x => new StateHistoryDto
            {
                StateName = x.Name,
                CreatedAt = x.CreatedAt,
                Reason = x.Reason,
                Data = x.Data
            })
                .Reverse()
                .ToList();

            return new JobDetailsDto
            {
                CreatedAt = job.CreatedAt,
                Job = DeserializeJob(job.InvocationData, job.Arguments),
                History = history,
                Properties = job.Parameters
            };
        }

        private static readonly BsonArray StatisticsStateNames = new BsonArray
        {
            EnqueuedState.StateName,
            FailedState.StateName,
            ProcessingState.StateName,
            ScheduledState.StateName
        };

        public override StatisticsDto GetStatistics()
        {
            var stats = new StatisticsDto();
            var filter = new BsonDocument
            {
                ["_t"] = nameof(JobDto),
                [nameof(JobDto.StateName)] = new BsonDocument
                {
                    ["$in"] = StatisticsStateNames
                }

            };
            var pipeline = new BsonDocument[]
            {
                new BsonDocument("$match", filter),
                new BsonDocument("$group",
                new BsonDocument
                    {
                        { "_id", $"${nameof(JobDto.StateName)}" },
                        { "count", new BsonDocument("$sum", 1) }
                    })
            };

            var countByStates = _dbContext
                .JobGraph
                .Aggregate<BsonDocument>(pipeline)
                .ToList()
                .ToDictionary(b => b["_id"].ToString(), b => b["count"].AsInt32);

            int GetCountIfExists(string name) => countByStates.ContainsKey(name) ? countByStates[name] : 0;

            stats.Enqueued = GetCountIfExists(EnqueuedState.StateName);
            stats.Failed = GetCountIfExists(FailedState.StateName);
            stats.Processing = GetCountIfExists(ProcessingState.StateName);
            stats.Scheduled = GetCountIfExists(ScheduledState.StateName);

            stats.Servers = _dbContext.Server.Count(new BsonDocument());

            var statsSucceeded = $@"stats:{State.Succeeded}";
            filter = new BsonDocument
            {
                ["_t"] = nameof(CounterDto),
                [nameof(KeyJobDto.Key)] = statsSucceeded
            };
            var succeededCounter = _dbContext.JobGraph.Find(filter).FirstOrDefault();

            stats.Succeeded = succeededCounter?.GetElement(nameof(CounterDto.Value)).Value.AsInt64 ?? 0;

            var statsDeleted = $@"stats:{State.Deleted}";
            filter = new BsonDocument
            {
                ["_t"] = nameof(CounterDto),
                [nameof(KeyJobDto.Key)] = statsDeleted
            };
            var deletedCounter = _dbContext.JobGraph.Find(filter).FirstOrDefault();

            stats.Deleted = deletedCounter?.GetElement(nameof(CounterDto.Value)).Value.AsInt64 ?? 0;
            filter = new BsonDocument
            {
                [nameof(KeyJobDto.Key)] = new BsonDocument("$regex", "^recurring-jobs"),
                ["_t"] = nameof(SetDto)
            };
            stats.Recurring = _dbContext.JobGraph.Count(filter);

            stats.Queues = GetQueues().Count;

            return stats;
        }

        public override JobList<EnqueuedJobDto> EnqueuedJobs(string queue, int from, int perPage)
        {
            var enqueuedJobIds = GetEnqueuedJobIds(queue, from, perPage);

            return EnqueuedJobs(enqueuedJobIds);
        }

        public override JobList<FetchedJobDto> FetchedJobs(string queue, int from, int perPage)
        {
            var fetchedJobIds = GetFetchedJobIds(queue, from, perPage);

            return FetchedJobs(_dbContext, fetchedJobIds);
        }

        public override JobList<ProcessingJobDto> ProcessingJobs(int from, int count)
        {
            return GetJobs(from, count,
                ProcessingState.StateName,
                (jobSummary, job, invocationData, loadException, stateData) => new ProcessingJobDto
                {
                    Job = job,
                    LoadException = loadException,
                    InvocationData = invocationData,
                    InProcessingState = ProcessingState.StateName.Equals(jobSummary.StateName, StringComparison.OrdinalIgnoreCase),
                    ServerId = stateData.ContainsKey("ServerId") ? stateData["ServerId"] : stateData["ServerName"],
                    StartedAt = jobSummary.StateChanged,
                    StateData = stateData
                });
        }

        public override JobList<ScheduledJobDto> ScheduledJobs(int from, int count)
        {
            return GetJobs(from, count, ScheduledState.StateName,
                (sqlJob, job, invocationData, loadException, stateData) => new ScheduledJobDto
                {
                    Job = job,
                    LoadException = loadException,
                    InvocationData = invocationData,
                    InScheduledState = ScheduledState.StateName.Equals(sqlJob.StateName, StringComparison.OrdinalIgnoreCase),
                    EnqueueAt = JobHelper.DeserializeNullableDateTime(stateData["EnqueueAt"]) ?? DateTime.MinValue,
                    ScheduledAt = sqlJob.StateChanged,
                    StateData = stateData
                });
        }

        public override JobList<SucceededJobDto> SucceededJobs(int from, int count)
        {
            return GetJobs(from, count, SucceededState.StateName,
                (sqlJob, job, invocationData, loadException, stateData) => new SucceededJobDto
                {
                    Job = job,
                    LoadException = loadException,
                    InvocationData = invocationData,
                    InSucceededState = SucceededState.StateName.Equals(sqlJob.StateName, StringComparison.OrdinalIgnoreCase),
                    Result = stateData["Result"],
                    TotalDuration = stateData.ContainsKey("PerformanceDuration") && stateData.ContainsKey("Latency")
                        ? (long?)long.Parse(stateData["PerformanceDuration"]) + (long?)long.Parse(stateData["Latency"])
                        : null,
                    SucceededAt = sqlJob.StateChanged,
                    StateData = stateData
                });
        }

        public override JobList<FailedJobDto> FailedJobs(int from, int count)
        {
            return GetJobs(from, count, FailedState.StateName,
                (sqlJob, job, invocationData, loadException, stateData) => new FailedJobDto
                {
                    Job = job,
                    LoadException = loadException,
                    InvocationData = invocationData,
                    InFailedState = FailedState.StateName.Equals(sqlJob.StateName, StringComparison.OrdinalIgnoreCase),
                    Reason = sqlJob.StateReason,
                    ExceptionDetails = stateData["ExceptionDetails"],
                    ExceptionMessage = stateData["ExceptionMessage"],
                    ExceptionType = stateData["ExceptionType"],
                    FailedAt = sqlJob.StateChanged,
                    StateData = stateData
                });
        }

        public override JobList<DeletedJobDto> DeletedJobs(int from, int count)
        {
            return GetJobs(from, count, DeletedState.StateName,
                (sqlJob, job, invocationData, loadException, stateData) => new DeletedJobDto
                {
                    Job = job,
                    LoadException = loadException,
                    InvocationData = invocationData,
                    InDeletedState = DeletedState.StateName.Equals(sqlJob.StateName, StringComparison.OrdinalIgnoreCase),
                    DeletedAt = sqlJob.StateChanged,
                    StateData = stateData
                });
        }

        public override JobList<AwaitingJobDto> AwaitingJobs(int from, int count)
        {
            return GetJobs(from, count, AwaitingState.StateName,
                (sqlJob, job, invocationData, loadException, stateData) => new AwaitingJobDto
                {
                    Job = job,
                    LoadException = loadException,
                    InvocationData = invocationData,
                    InAwaitingState = AwaitingState.StateName.Equals(sqlJob.StateName, StringComparison.OrdinalIgnoreCase),
                    AwaitingAt = sqlJob.StateChanged,
                    StateData = stateData

                });
        }

        

        public override long ScheduledCount()
        {
            return GetNumberOfJobsByStateName(ScheduledState.StateName);
        }

        public override long EnqueuedCount(string queue)
        {
            var counters = GetEnqueuedAndFetchedCount(queue);

            return counters.EnqueuedCount ?? 0;
        }

        public override long FetchedCount(string queue)
        {
            var counters = GetEnqueuedAndFetchedCount(queue);

            return counters.FetchedCount ?? 0;
        }

        public override long FailedCount()
        {
            return GetNumberOfJobsByStateName(FailedState.StateName);
        }

        public override long ProcessingCount()
        {
            return GetNumberOfJobsByStateName(ProcessingState.StateName);
        }

        public override long SucceededListCount()
        {
            return GetNumberOfJobsByStateName(SucceededState.StateName);
        }

        public override long DeletedListCount()
        {
            return GetNumberOfJobsByStateName(DeletedState.StateName);
        }

        public override long AwaitingCount()
        {
            return GetNumberOfJobsByStateName(AwaitingState.StateName);
        }

        public override IDictionary<DateTime, long> SucceededByDatesCount()
        {
            return GetTimelineStats(State.Succeeded);
        }

        public override IDictionary<DateTime, long> FailedByDatesCount()
        {
            return GetTimelineStats(State.Failed);
        }

        public override IDictionary<DateTime, long> HourlySucceededJobs()
        {
            return GetHourlyTimelineStats(State.Succeeded);
        }

        public override IDictionary<DateTime, long> HourlyFailedJobs()
        {
            return GetHourlyTimelineStats(State.Failed);
        }

        public override IDictionary<DateTime, long> DeletedByDatesCount()
        {
            return GetTimelineStats(State.Deleted);
        }

        public override IDictionary<DateTime, long> HourlyDeletedJobs()
        {
            return GetHourlyTimelineStats(State.Deleted);
        }

        public virtual IReadOnlyList<string> GetQueues()
        {
            var filter = new BsonDocument
            {
                ["_t"] = nameof(JobDto),
                [nameof(JobDto.Queue)] = new BsonDocument
                {
                    ["$ne"] = BsonNull.Value
                }
            };
            var pipeline = new BsonDocument[]
            {
                new BsonDocument("$match",
                new BsonDocument("_t", nameof(JobDto))),
                new BsonDocument("$group", new BsonDocument("_id", $"${nameof(JobDto.Queue)}"))
            };
            return _dbContext.JobGraph
            .Aggregate<BsonDocument>(pipeline)
                .ToList()
                .Where(b => b["_id"] != BsonNull.Value)
                .Select(b => b["_id"].AsString)
                .ToList();
        }

        public virtual IReadOnlyList<string> GetEnqueuedJobIds(string queue, int from, int perPage)
        {
            var filter = new BsonDocument
            {
                ["_t"] = nameof(JobDto),
                [nameof(JobDto.Queue)] = queue,
                [nameof(JobDto.FetchedAt)] = BsonNull.Value,
            };
            return _dbContext.JobGraph
                .Find(filter)
                .Skip(from)
                .Limit(perPage)
                .Project(new BsonDocument("_id", 1))
                .ToList()
                .Select(b => b["_id"].ToString())
                .ToList();

        }

        public virtual IReadOnlyList<string> GetFetchedJobIds(string queue, int from, int perPage)
        {
            var filter = new BsonDocument
            {
                ["_t"] = nameof(JobDto),
                [nameof(JobDto.Queue)] = queue,
                [nameof(JobDto.FetchedAt)] = new BsonDocument
                {
                    ["$ne"] = BsonNull.Value
                }
            };
            return _dbContext.JobGraph
                .Find(filter)
                .Skip(from)
                .Limit(perPage)
                .Project(new BsonDocument("_id", 1))
                .ToList()
                .Select(b => b["_id"].ToString())
                .ToList();
        }

        public virtual EnqueuedAndFetchedCountDto GetEnqueuedAndFetchedCount(string queue)
        {
            var nonFetched = new BsonDocument
            {
                ["_t"] = nameof(JobDto),
                [nameof(JobDto.Queue)] = queue,
                [nameof(JobDto.FetchedAt)] = BsonNull.Value,
            };
            int enqueuedCount = (int)_dbContext.JobGraph.Count(nonFetched);

            var fetched = new BsonDocument
            {
                ["_t"] = nameof(JobDto),
                [nameof(JobDto.Queue)] = queue,
                [nameof(JobDto.FetchedAt)] = new BsonDocument
                {
                    ["$ne"] = BsonNull.Value
                }
            };

            int fetchedCount = (int)_dbContext.JobGraph.Count(fetched);

            return new EnqueuedAndFetchedCountDto
            {
                EnqueuedCount = enqueuedCount,
                FetchedCount = fetchedCount
            };
        }

        public virtual JobList<EnqueuedJobDto> EnqueuedJobs(IEnumerable<string> jobIds)
        {
            var jobObjectIds = new BsonArray(jobIds.Select(ObjectId.Parse));

            var jobsByIdsFilter = new BsonDocument
            {
                ["_t"] = nameof(JobDto),
                [nameof(JobDto.Queue)] = new BsonDocument("$ne", BsonNull.Value),
                [nameof(JobDto.FetchedAt)] = BsonNull.Value,
                ["_id"] = new BsonDocument
                {
                    ["$in"] = jobObjectIds
                }
            };
            var jobs = _dbContext
                .JobGraph
                .Find(jobsByIdsFilter)
                .ToList()
                .Select(b => new JobDto(b));

            var enqueuedJobs = jobs
                .Select(job =>
                {
                    var state = job.StateHistory.LastOrDefault();
                    return new JobSummary
                    {
                        Id = job.Id.ToString(),
                        InvocationData = job.InvocationData,
                        Arguments = job.Arguments,
                        CreatedAt = job.CreatedAt,
                        ExpireAt = job.ExpireAt,
                        FetchedAt = null,
                        StateName = job.StateName,
                        StateReason = state?.Reason,
                        StateData = state?.Data,
                        StateChanged = job.StateChanged
                    };
                })
                .ToList();

            return DeserializeJobs(
                enqueuedJobs,
                (jobSummary, job, invocationData, loadException, stateData) => new EnqueuedJobDto
                {
                    Job = job,
                    State = jobSummary.StateName,
                    LoadException = loadException,
                    InvocationData = invocationData,
                    InEnqueuedState = EnqueuedState.StateName.Equals(jobSummary.StateName, StringComparison.OrdinalIgnoreCase),
                    EnqueuedAt = EnqueuedState.StateName.Equals(jobSummary.StateName, StringComparison.OrdinalIgnoreCase)
                        ? jobSummary.StateChanged
                        : null,
                    StateData = stateData
                });
        }

        private static JobList<TDto> DeserializeJobs<TDto>(ICollection<JobSummary> jobs,
            Func<JobSummary, Job, InvocationData, JobLoadException, SafeDictionary<string, string>, TDto> selector)
        {
            var result = new List<KeyValuePair<string, TDto>>(jobs.Count);
            foreach (var job in jobs)
            {
                var dto = default(TDto);

                if (job.InvocationData != null)
                {
                    var deserializedData = job.StateData;
                    var stateData = deserializedData != null
                        ? new SafeDictionary<string, string>(deserializedData, StringComparer.OrdinalIgnoreCase)
                        : null;

                    dto = selector(job, DeserializeJob(
                        job.InvocationData, 
                        job.Arguments, 
                        out var invocationData, 
                        out var loadException), 
                        invocationData, 
                        loadException, 
                        stateData);
                }

                result.Add(new KeyValuePair<string, TDto>(
                    job.Id.ToString(), dto));
            }

            return new JobList<TDto>(result);
        }

        public static Job DeserializeJob(
            string invocationData, 
            string arguments, out InvocationData data, out JobLoadException exception)
        {
            data = InvocationData.DeserializePayload(invocationData);

            if (!string.IsNullOrEmpty(arguments))
            {
                data.Arguments = arguments;
            }

            try
            {
                exception = null;
                return data.DeserializeJob();
            }
            catch (JobLoadException ex)
            {
                exception = ex;
                return null;
            }
        }

        public static Job DeserializeJob(string invocationData, string arguments)
        {
            var data = JobHelper.FromJson<InvocationData>(invocationData);
            data.Arguments = arguments;

            try
            {
                return data.Deserialize();
            }
            catch (JobLoadException)
            {
                return null;
            }
        }

        public virtual JobList<FetchedJobDto> FetchedJobs(HangfireDbContext connection, IEnumerable<string> jobIds)
        {
            if (!jobIds.Any())
            {
                return new JobList<FetchedJobDto>(new List<KeyValuePair<string, FetchedJobDto>>());
            }

            var jobObjectIds = new BsonArray(jobIds.Select(ObjectId.Parse));
            var filter = new BsonDocument
            {
                ["_t"] = nameof(JobDto),
                [nameof(JobDto.FetchedAt)] = new BsonDocument("$ne", BsonNull.Value),
                [nameof(JobDto.Queue)] = new BsonDocument("$ne", BsonNull.Value),
                ["_id"] = new BsonDocument
                {
                    ["$in"] = jobObjectIds
                }
            };
            var jobs = connection
                .JobGraph
                .Find(filter)
                .ToList()
                .Select(b => new JobDto(b));

            var fetcedJobs = jobs
                .Select(job =>
                {
                    var state = job.StateHistory.LastOrDefault(s => s.Name == job.StateName);
                    return new JobSummary
                    {
                        Id = job.Id.ToString(),
                        InvocationData = job.InvocationData,
                        Arguments = job.Arguments,
                        CreatedAt = job.CreatedAt,
                        ExpireAt = job.ExpireAt,
                        FetchedAt = null,
                        StateName = job.StateName,
                        StateReason = state?.Reason,
                        StateData = state?.Data
                    };
                })
                .ToList();

            var result = new List<KeyValuePair<string, FetchedJobDto>>(fetcedJobs.Count);

            foreach (var job in fetcedJobs)
            {
                result.Add(new KeyValuePair<string, FetchedJobDto>(job.Id,
                    new FetchedJobDto
                    {
                        Job = DeserializeJob(job.InvocationData, job.Arguments),
                        State = job.StateName,
                        FetchedAt = job.FetchedAt
                    }));
            }

            return new JobList<FetchedJobDto>(result);
        }

        private JobList<TDto> GetJobs<TDto>(int from, int count, string stateName,
            Func<JobSummary, Job, InvocationData, JobLoadException, SafeDictionary<string, string>, TDto> selector)
        {
            // only retrieve job ids
            var filter = new BsonDocument
            {
                ["_t"] = nameof(JobDto),
                [nameof(JobDto.StateName)] = stateName
            };

            var jobs = _dbContext
                .JobGraph
                .Find(filter)
                .Sort(new BsonDocument("_id", -1))
                .Skip(from)
                .Limit(count)
                .ToList()
                .Select(b => new JobDto(b));

            var joinedJobs = jobs
                .Select(job =>
                {
                    var state = job.StateHistory.LastOrDefault(s => s.Name == stateName);
                    return new JobSummary
                    {
                        Id = job.Id.ToString(),
                        InvocationData = job.InvocationData,
                        Arguments = job.Arguments,
                        CreatedAt = job.CreatedAt,
                        ExpireAt = job.ExpireAt,
                        FetchedAt = null,
                        StateName = job.StateName,
                        StateReason = state?.Reason,
                        StateData = state?.Data
                    };
                })
                .ToList();

            return DeserializeJobs(joinedJobs, selector);
        }

        public virtual long GetNumberOfJobsByStateName(string stateName)
        {
            var filter = new BsonDocument
            {
                ["_t"] = nameof(JobDto),
                [nameof(JobDto.StateName)] = stateName
            };
            var count = _dbContext.JobGraph.Count(filter);
            return count;
        }

        public virtual Dictionary<DateTime, long> GetTimelineStats(string type)
        {
            var endDate = DateTime.UtcNow.Date;
            var startDate = endDate.AddDays(-7);
            var dates = new List<DateTime>();

            while (startDate <= endDate)
            {
                dates.Add(endDate);
                endDate = endDate.AddDays(-1);
            }

            var stringDates = dates.Select(x => x.ToString("yyyy-MM-dd")).ToList();
            var keys = stringDates.Select(x => $"stats:{type}:{x}");

            return CreateTimeLineStats(keys, dates);
        }

        public virtual Dictionary<DateTime, long> GetHourlyTimelineStats(string type)
        {
            var endDate = DateTime.UtcNow;
            var dates = new List<DateTime>();
            for (var i = 0; i < 24; i++)
            {
                dates.Add(endDate);
                endDate = endDate.AddHours(-1);
            }

            var keys = dates.Select(x => $"stats:{type}:{x:yyyy-MM-dd-HH}");

            return CreateTimeLineStats(keys, dates);
        }

        public virtual Dictionary<DateTime, long> CreateTimeLineStats(IEnumerable<string> keys, IList<DateTime> dates)
        {
            var bsonKeys = BsonArray.Create(keys);
            var filter = new BsonDocument
            {
                ["_t"] = nameof(CounterDto),
                [nameof(CounterDto.Key)] = new BsonDocument
                {
                    ["$in"] = bsonKeys
                }
            };
            var valuesMap = _dbContext.JobGraph
                .Find(filter)
                .ToList()
                .Select(b => new CounterDto(b))
                .GroupBy(counter => counter.Key, counter => counter)
                .ToDictionary(counter => counter.Key, grouping => grouping.Sum(c => c.Value));

            foreach (var key in keys.Where(key => !valuesMap.ContainsKey(key)))
            {
                valuesMap.Add(key, 0);
            }

            var result = new Dictionary<DateTime, long>();
            for (var i = 0; i < dates.Count; i++)
            {
                var value = valuesMap[valuesMap.Keys.ElementAt(i)];
                result.Add(dates[i], value);
            }

            return result;
        }
        /// <summary>
        /// Overloaded dictionary that doesn't throw if given an invalid key
        /// Fixes issues such as https://github.com/HangfireIO/Hangfire/issues/871
        /// </summary>
        private class SafeDictionary<TKey, TValue> : Dictionary<TKey, TValue>
        {
            public SafeDictionary(IDictionary<TKey, TValue> dictionary, IEqualityComparer<TKey> comparer)
                : base(dictionary, comparer)
            {
            }

            public new TValue this[TKey i]
            {
                get { return ContainsKey(i) ? base[i] : default(TValue); }
                set { base[i] = value; }
            }
        }

        private class ParentStateDto
        {
            public long Id { get; set; }
            public string StateName { get; set; }
        }
    }
#pragma warning restore 1591
}