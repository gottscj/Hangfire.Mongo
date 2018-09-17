using System;
using System.Collections.Generic;
using System.Linq;
using Hangfire.Common;
using Hangfire.Mongo.Database;
using Hangfire.Mongo.Dto;
using Hangfire.Mongo.PersistentJobQueue;
using Hangfire.States;
using Hangfire.Storage;
using Hangfire.Storage.Monitoring;
using MongoDB.Bson;
using MongoDB.Driver;
using ServerDto = Hangfire.Storage.Monitoring.ServerDto;

namespace Hangfire.Mongo
{
#pragma warning disable 1591
    public class MongoMonitoringApi : IMonitoringApi
    {
        private readonly HangfireDbContext _dbContext;

        private readonly PersistentJobQueueProviderCollection _queueProviders;

        public MongoMonitoringApi(HangfireDbContext dbContext, PersistentJobQueueProviderCollection queueProviders)
        {
            _dbContext = dbContext;
            _queueProviders = queueProviders;
        }

        public IList<QueueWithTopEnqueuedJobsDto> Queues()
        {
            var tuples = _queueProviders
                .Select(x => x.GetJobQueueMonitoringApi(_dbContext))
                .SelectMany(x => x.GetQueues(), (monitoring, queue) => new {Monitoring = monitoring, Queue = queue})
                .OrderBy(x => x.Queue)
                .ToArray();

            var result = new List<QueueWithTopEnqueuedJobsDto>(tuples.Length);

            foreach (var tuple in tuples)
            {
                var enqueuedJobIds = tuple.Monitoring.GetEnqueuedJobIds(tuple.Queue, 0, 5);
                var counters = tuple.Monitoring.GetEnqueuedAndFetchedCount(tuple.Queue);

                result.Add(new QueueWithTopEnqueuedJobsDto
                {
                    Name = tuple.Queue,
                    Length = counters.EnqueuedCount ?? 0,
                    Fetched = counters.FetchedCount,
                    FirstJobs = EnqueuedJobs(enqueuedJobIds)
                });
            }

            return result;
        }

        public IList<ServerDto> Servers()
        {
            var servers = _dbContext.Server.Find(new BsonDocument()).ToList();

            var result = new List<ServerDto>();

            foreach (var server in servers)
            {
                var data = JobHelper.FromJson<ServerDataDto>(server.Data);
                result.Add(new ServerDto
                {
                    Name = server.Id,
                    Heartbeat = server.LastHeartbeat,
                    Queues = data.Queues,
                    StartedAt = data.StartedAt ?? DateTime.MinValue,
                    WorkersCount = data.WorkerCount
                });
            }

            return result;
        }

        public JobDetailsDto JobDetails(string jobId)
        {
            JobDto job = _dbContext
                .JobGraph
                .OfType<JobDto>()
                .Find(Builders<JobDto>.Filter.Eq(_ => _.Id, ObjectId.Parse(jobId)))
                .FirstOrDefault();

            if (job == null)
                return null;

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

        private static readonly string[] StatisticsStateNames = new[]
        {
            EnqueuedState.StateName,
            FailedState.StateName,
            ProcessingState.StateName,
            ScheduledState.StateName
        };

        public StatisticsDto GetStatistics()
        {
            var stats = new StatisticsDto();

            var countByStates = _dbContext
                .JobGraph
                .OfType<JobDto>()
                .Aggregate()
                .Match(Builders<JobDto>.Filter.In(_ => _.StateName, StatisticsStateNames))
                .Group(dto => new {dto.StateName},
                    dtos => new {StateName = dtos.First().StateName, Count = dtos.Count()})
                .ToList().ToDictionary(kv => kv.StateName, kv => kv.Count);

            int GetCountIfExists(string name) => countByStates.ContainsKey(name) ? countByStates[name] : 0;

            stats.Enqueued = GetCountIfExists(EnqueuedState.StateName);
            stats.Failed = GetCountIfExists(FailedState.StateName);
            stats.Processing = GetCountIfExists(ProcessingState.StateName);
            stats.Scheduled = GetCountIfExists(ScheduledState.StateName);

            stats.Servers = _dbContext.Server.Count(new BsonDocument());

            var statsSucceeded = $@"stats:{State.Succeeded}";
            var succeededCounter = _dbContext.JobGraph.OfType<CounterDto>()
                .Find(new BsonDocument(nameof(KeyJobDto.Key), statsSucceeded))
                .FirstOrDefault();
                
            stats.Succeeded = succeededCounter?.Value ?? 0;

            var statsDeleted = $@"stats:{State.Deleted}";
            var deletedCounter = _dbContext.JobGraph.OfType<CounterDto>()
                .Find(new BsonDocument(nameof(KeyJobDto.Key), statsDeleted))
                .FirstOrDefault();
            
            stats.Deleted = deletedCounter?.Value ?? 0;

            stats.Recurring = _dbContext
                .JobGraph
                .OfType<SetDto>()
                .Count(new BsonDocument(nameof(KeyJobDto.Key), "recurring-jobs"));

            stats.Queues = _queueProviders
                .SelectMany(x => x.GetJobQueueMonitoringApi(_dbContext).GetQueues())
                .Count();

            return stats;
        }

        public JobList<EnqueuedJobDto> EnqueuedJobs(string queue, int from, int perPage)
        {
            var queueApi = GetQueueApi(queue);
            var enqueuedJobIds = queueApi.GetEnqueuedJobIds(queue, from, perPage);

            return EnqueuedJobs(enqueuedJobIds);
        }

        public JobList<FetchedJobDto> FetchedJobs(string queue, int from, int perPage)
        {
            var queueApi = GetQueueApi(queue);
            var fetchedJobIds = queueApi.GetFetchedJobIds(queue, from, perPage);

            return FetchedJobs(_dbContext, fetchedJobIds);
        }

        public JobList<ProcessingJobDto> ProcessingJobs(int from, int count)
        {
            return GetJobs(from, count,
                ProcessingState.StateName,
                (sqlJob, job, stateData) => new ProcessingJobDto
                {
                    Job = job,
                    ServerId = stateData.ContainsKey("ServerId") ? stateData["ServerId"] : stateData["ServerName"],
                    StartedAt = JobHelper.DeserializeDateTime(stateData["StartedAt"]),
                });
        }

        public JobList<ScheduledJobDto> ScheduledJobs(int from, int count)
        {
            return GetJobs(from, count, ScheduledState.StateName,
                (sqlJob, job, stateData) => new ScheduledJobDto
                {
                    Job = job,
                    EnqueueAt = JobHelper.DeserializeDateTime(stateData["EnqueueAt"]),
                    ScheduledAt = JobHelper.DeserializeDateTime(stateData["ScheduledAt"])
                });
        }

        public JobList<SucceededJobDto> SucceededJobs(int from, int count)
        {
            return GetJobs(from, count, SucceededState.StateName,
                (sqlJob, job, stateData) => new SucceededJobDto
                {
                    Job = job,
                    Result = stateData.ContainsKey("Result") ? stateData["Result"] : null,
                    TotalDuration = stateData.ContainsKey("PerformanceDuration") && stateData.ContainsKey("Latency")
                        ? (long?) long.Parse(stateData["PerformanceDuration"]) +
                          (long?) long.Parse(stateData["Latency"])
                        : null,
                    SucceededAt = JobHelper.DeserializeNullableDateTime(stateData["SucceededAt"])
                });
        }

        public JobList<FailedJobDto> FailedJobs(int from, int count)
        {
            return GetJobs(from, count, FailedState.StateName,
                (sqlJob, job, stateData) => new FailedJobDto
                {
                    Job = job,
                    Reason = sqlJob.StateReason,
                    ExceptionDetails = stateData["ExceptionDetails"],
                    ExceptionMessage = stateData["ExceptionMessage"],
                    ExceptionType = stateData["ExceptionType"],
                    FailedAt = JobHelper.DeserializeNullableDateTime(stateData["FailedAt"])
                });
        }

        public JobList<DeletedJobDto> DeletedJobs(int from, int count)
        {
            return GetJobs(from, count, DeletedState.StateName,
                (sqlJob, job, stateData) => new DeletedJobDto
                {
                    Job = job,
                    DeletedAt = JobHelper.DeserializeNullableDateTime(stateData["DeletedAt"])
                });
        }

        public long ScheduledCount()
        {
            return GetNumberOfJobsByStateName(ScheduledState.StateName);
        }

        public long EnqueuedCount(string queue)
        {
            var queueApi = GetQueueApi(queue);
            var counters = queueApi.GetEnqueuedAndFetchedCount(queue);

            return counters.EnqueuedCount ?? 0;
        }

        public long FetchedCount(string queue)
        {
            var queueApi = GetQueueApi(queue);
            var counters = queueApi.GetEnqueuedAndFetchedCount(queue);

            return counters.FetchedCount ?? 0;
        }

        public long FailedCount()
        {
            return GetNumberOfJobsByStateName(FailedState.StateName);
        }

        public long ProcessingCount()
        {
            return GetNumberOfJobsByStateName(ProcessingState.StateName);
        }

        public long SucceededListCount()
        {
            return GetNumberOfJobsByStateName(SucceededState.StateName);
        }

        public long DeletedListCount()
        {
            return GetNumberOfJobsByStateName(DeletedState.StateName);
        }

        public IDictionary<DateTime, long> SucceededByDatesCount()
        {
            return GetTimelineStats(State.Succeeded);
        }

        public IDictionary<DateTime, long> FailedByDatesCount()
        {
            return GetTimelineStats(State.Failed);
        }

        public IDictionary<DateTime, long> HourlySucceededJobs()
        {
            return GetHourlyTimelineStats(State.Succeeded);
        }

        public IDictionary<DateTime, long> HourlyFailedJobs()
        {
            return GetHourlyTimelineStats(State.Failed);
        }

        private JobList<EnqueuedJobDto> EnqueuedJobs(IEnumerable<string> jobIds)
        {
            var jobObjectIds = jobIds.Select(ObjectId.Parse);
            var jobs = _dbContext
                .JobGraph
                .OfType<JobDto>()
                .Find(Builders<JobDto>.Filter.In(_ => _.Id, jobObjectIds))
                .ToList();

            var filterBuilder = Builders<JobQueueDto>.Filter;
            var enqueuedJobs = _dbContext
                .JobGraph
                .OfType<JobQueueDto>()
                .Find(filterBuilder.In(_ => _.JobId, jobs.Select(job => job.Id)) &
                      (filterBuilder.Not(filterBuilder.Exists(_ => _.FetchedAt)) |
                       filterBuilder.Eq(_ => _.FetchedAt, null)))
                .ToList();

            var jobsFiltered = enqueuedJobs
                .Select(jq => jobs.FirstOrDefault(job => job.Id == jq.JobId));

            var joinedJobs = jobsFiltered
                .Where(job => job != null)
                .Select(job =>
                {
                    var state = job.StateHistory.LastOrDefault();
                    return new JobDetailedDto
                    {
                        Id = job.Id,
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

            return DeserializeJobs(
                joinedJobs,
                (sqlJob, job, stateData) => new EnqueuedJobDto
                {
                    Job = job,
                    State = sqlJob.StateName,
                    EnqueuedAt = sqlJob.StateName == EnqueuedState.StateName
                        ? JobHelper.DeserializeNullableDateTime(stateData["EnqueuedAt"])
                        : null
                });
        }

        private static JobList<TDto> DeserializeJobs<TDto>(ICollection<JobDetailedDto> jobs,
            Func<JobDetailedDto, Job, Dictionary<string, string>, TDto> selector)
        {
            var result = new List<KeyValuePair<string, TDto>>(jobs.Count);

            foreach (var job in jobs)
            {
                var stateData = job.StateData;
                var dto = selector(job, DeserializeJob(job.InvocationData, job.Arguments), stateData);
                result.Add(new KeyValuePair<string, TDto>(job.Id.ToString(), dto));
            }

            return new JobList<TDto>(result);
        }

        private static Job DeserializeJob(string invocationData, string arguments)
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

        private IPersistentJobQueueMonitoringApi GetQueueApi(string queueName)
        {
            var provider = _queueProviders.GetProvider(queueName);
            var monitoringApi = provider.GetJobQueueMonitoringApi(_dbContext);

            return monitoringApi;
        }

        private JobList<FetchedJobDto> FetchedJobs(HangfireDbContext connection, IEnumerable<string> jobIds)
        {
            var jobObjectIds = jobIds.Select(ObjectId.Parse);
            var jobs = connection
                .JobGraph
                .OfType<JobDto>()
                .Find(Builders<JobDto>.Filter.In(_ => _.Id, jobObjectIds))
                .ToList();

            var jobIdToJobQueueMap = connection.JobGraph.OfType<JobQueueDto>()
                .Find(Builders<JobQueueDto>.Filter.In(_ => _.JobId, jobs.Select(job => job.Id))
                      & Builders<JobQueueDto>.Filter.Exists(_ => _.FetchedAt)
                      & Builders<JobQueueDto>.Filter.Not(Builders<JobQueueDto>.Filter.Eq(_ => _.FetchedAt, null)))
                .ToList().ToDictionary(kv => kv.JobId, kv => kv);

            IEnumerable<JobDto> jobsFiltered = jobs.Where(job => jobIdToJobQueueMap.ContainsKey(job.Id));

            List<JobDetailedDto> joinedJobs = jobsFiltered
                .Select(job =>
                {
                    var state = job.StateHistory.FirstOrDefault(s => s.Name == job.StateName);
                    return new JobDetailedDto
                    {
                        Id = job.Id,
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

            var result = new List<KeyValuePair<string, FetchedJobDto>>(joinedJobs.Count);

            foreach (var job in joinedJobs)
            {
                result.Add(new KeyValuePair<string, FetchedJobDto>(
                    job.Id.ToString(),
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
            Func<JobDetailedDto, Job, Dictionary<string, string>, TDto> selector)
        {
            // only retrieve job ids
            var filter = Builders<JobDto>
                .Filter
                .Eq(j => j.StateName, stateName);

            var jobs = _dbContext
                .JobGraph
                .OfType<JobDto>()
                .Find(filter)
                .SortByDescending(_ => _.Id)
                .Skip(from)
                .Limit(count)
                .ToList();

            var joinedJobs = jobs
                .Select(job =>
                {
                    var state = job.StateHistory.FirstOrDefault(s => s.Name == stateName);

                    return new JobDetailedDto
                    {
                        Id = job.Id,
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

        private long GetNumberOfJobsByStateName(string stateName)
        {
            var count = _dbContext.JobGraph.OfType<JobDto>().Count(Builders<JobDto>.Filter.Eq(_ => _.StateName, stateName));
            return count;
        }

        private Dictionary<DateTime, long> GetTimelineStats(string type)
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

        private Dictionary<DateTime, long> GetHourlyTimelineStats(string type)
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

        private Dictionary<DateTime, long> CreateTimeLineStats(IEnumerable<string> keys, IList<DateTime> dates)
        {
            var bsonKeys = BsonArray.Create(keys);
            var valuesMap = _dbContext.JobGraph.OfType<CounterDto>()
                .Find(new BsonDocument(nameof(CounterDto.Key), new BsonDocument("$in", bsonKeys)))
                .ToList()
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
    }
#pragma warning restore 1591
}