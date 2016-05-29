using System;
using System.Collections.Generic;
using System.Linq;
using Hangfire.Common;
using Hangfire.Mongo.Database;
using Hangfire.Mongo.Dto;
using Hangfire.Mongo.MongoUtils;
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
        private readonly HangfireDbContext _database;

        private readonly PersistentJobQueueProviderCollection _queueProviders;

        public MongoMonitoringApi(HangfireDbContext database, PersistentJobQueueProviderCollection queueProviders)
        {
            _database = database;
            _queueProviders = queueProviders;
        }

        public IList<QueueWithTopEnqueuedJobsDto> Queues()
        {
            return UseConnection<IList<QueueWithTopEnqueuedJobsDto>>(connection =>
            {
                var tuples = _queueProviders
                    .Select(x => x.GetJobQueueMonitoringApi(connection))
                    .SelectMany(x => x.GetQueues(), (monitoring, queue) => new { Monitoring = monitoring, Queue = queue })
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
                        FirstJobs = EnqueuedJobs(connection, enqueuedJobIds)
                    });
                }

                return result;
            });
        }

        public IList<ServerDto> Servers()
        {
            return UseConnection<IList<ServerDto>>(connection =>
            {
                var servers = connection.Server.Find(new BsonDocument()).ToList();

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
            });
        }

        public JobDetailsDto JobDetails(string jobId)
        {
            return UseConnection(connection =>
            {
                JobDto job = connection.Job.Find(Builders<JobDto>.Filter.Eq(_ => _.Id, int.Parse(jobId))).FirstOrDefault();

                if (job == null)
                    return null;

                Dictionary<string, string> parameters = connection.JobParameter
                    .Find(Builders<JobParameterDto>.Filter.Eq(_ => _.JobId, int.Parse(jobId)))
                    .ToList()
                    .ToDictionary(x => x.Name, x => x.Value);

                List<StateHistoryDto> history = connection.State
                    .Find(Builders<StateDto>.Filter.Eq(_ => _.JobId, int.Parse(jobId)))
                    .Sort(Builders<StateDto>.Sort.Descending(_ => _.CreatedAt))
                    .Project(x => new StateHistoryDto
                    {
                        StateName = x.Name,
                        CreatedAt = x.CreatedAt,
                        Reason = x.Reason,
                        Data = JobHelper.FromJson<Dictionary<string, string>>(x.Data)
                    })
                    .ToList();

                return new JobDetailsDto
                {
                    CreatedAt = job.CreatedAt,
                    Job = DeserializeJob(job.InvocationData, job.Arguments),
                    History = history,
                    Properties = parameters
                };
            });
        }

        public StatisticsDto GetStatistics()
        {
            return UseConnection(connection =>
            {
                var stats = new StatisticsDto();

                var countByStates = connection.Job.Aggregate()
                    .Match(Builders<JobDto>.Filter.Ne(_ => _.StateName, null))
                    .Group(dto => new { dto.StateName }, dtos => new { StateName = dtos.First().StateName, Count = dtos.Count() })
                    .ToList().ToDictionary(kv => kv.StateName, kv => kv.Count);

                Func<string, int> getCountIfExists = name => countByStates.ContainsKey(name) ? countByStates[name] : 0;

                stats.Enqueued = getCountIfExists(EnqueuedState.StateName);
                stats.Failed = getCountIfExists(FailedState.StateName);
                stats.Processing = getCountIfExists(ProcessingState.StateName);
                stats.Scheduled = getCountIfExists(ScheduledState.StateName);

                stats.Servers = connection.Server.Count(new BsonDocument());

                int[] succeededItems = connection.Counter.Find(Builders<CounterDto>.Filter.Eq(_ => _.Key, "stats:succeeded")).ToList().Select(_ => _.Value)
                    .Concat(connection.AggregatedCounter.Find(Builders<AggregatedCounterDto>.Filter.Eq(_ => _.Key, "stats:succeeded")).ToList().Select(_ => (int)_.Value))
                    .ToArray();
                stats.Succeeded = succeededItems.Any() ? succeededItems.Sum() : 0;

                int[] deletedItems = connection.Counter.Find(Builders<CounterDto>.Filter.Eq(_ => _.Key, "stats:deleted")).ToList().Select(_ => _.Value)
                    .Concat(connection.AggregatedCounter.Find(Builders<AggregatedCounterDto>.Filter.Eq(_ => _.Key, "stats:deleted")).ToList().Select(_ => (int)_.Value))
                    .ToArray();
                stats.Deleted = deletedItems.Any() ? deletedItems.Sum() : 0;

                stats.Recurring = connection.Set.Count(Builders<SetDto>.Filter.Eq(_ => _.Key, "recurring-jobs"));

                stats.Queues = _queueProviders
                    .SelectMany(x => x.GetJobQueueMonitoringApi(connection).GetQueues())
                    .Count();

                return stats;
            });
        }

        public JobList<EnqueuedJobDto> EnqueuedJobs(string queue, int @from, int perPage)
        {
            return UseConnection(connection =>
            {
                var queueApi = GetQueueApi(connection, queue);
                var enqueuedJobIds = queueApi.GetEnqueuedJobIds(queue, from, perPage);

                return EnqueuedJobs(connection, enqueuedJobIds);
            });
        }

        public JobList<FetchedJobDto> FetchedJobs(string queue, int @from, int perPage)
        {
            return UseConnection(connection =>
            {
                var queueApi = GetQueueApi(connection, queue);
                var fetchedJobIds = queueApi.GetFetchedJobIds(queue, from, perPage);

                return FetchedJobs(connection, fetchedJobIds);
            });
        }

        public JobList<ProcessingJobDto> ProcessingJobs(int @from, int count)
        {
            return UseConnection(connection => GetJobs(
                connection,
                from, count,
                ProcessingState.StateName,
                (sqlJob, job, stateData) => new ProcessingJobDto
                {
                    Job = job,
                    ServerId = stateData.ContainsKey("ServerId") ? stateData["ServerId"] : stateData["ServerName"],
                    StartedAt = JobHelper.DeserializeDateTime(stateData["StartedAt"]),
                }));
        }

        public JobList<ScheduledJobDto> ScheduledJobs(int @from, int count)
        {
            return UseConnection(connection => GetJobs(connection, from, count, ScheduledState.StateName,
                (sqlJob, job, stateData) => new ScheduledJobDto
                {
                    Job = job,
                    EnqueueAt = JobHelper.DeserializeDateTime(stateData["EnqueueAt"]),
                    ScheduledAt = JobHelper.DeserializeDateTime(stateData["ScheduledAt"])
                }));
        }

        public JobList<SucceededJobDto> SucceededJobs(int @from, int count)
        {
            return UseConnection(connection => GetJobs(connection, from, count, SucceededState.StateName,
                (sqlJob, job, stateData) => new SucceededJobDto
                {
                    Job = job,
                    Result = stateData.ContainsKey("Result") ? stateData["Result"] : null,
                    TotalDuration = stateData.ContainsKey("PerformanceDuration") && stateData.ContainsKey("Latency")
                        ? (long?)long.Parse(stateData["PerformanceDuration"]) + (long?)long.Parse(stateData["Latency"])
                        : null,
                    SucceededAt = JobHelper.DeserializeNullableDateTime(stateData["SucceededAt"])
                }));
        }

        public JobList<FailedJobDto> FailedJobs(int @from, int count)
        {
            return UseConnection(connection => GetJobs(connection, from, count, FailedState.StateName,
                (sqlJob, job, stateData) => new FailedJobDto
                {
                    Job = job,
                    Reason = sqlJob.StateReason,
                    ExceptionDetails = stateData["ExceptionDetails"],
                    ExceptionMessage = stateData["ExceptionMessage"],
                    ExceptionType = stateData["ExceptionType"],
                    FailedAt = JobHelper.DeserializeNullableDateTime(stateData["FailedAt"])
                }));
        }

        public JobList<DeletedJobDto> DeletedJobs(int @from, int count)
        {
            return UseConnection(connection => GetJobs(connection, from, count, DeletedState.StateName,
                (sqlJob, job, stateData) => new DeletedJobDto
                {
                    Job = job,
                    DeletedAt = JobHelper.DeserializeNullableDateTime(stateData["DeletedAt"])
                }));
        }

        public long ScheduledCount()
        {
            return UseConnection(connection => GetNumberOfJobsByStateName(connection, ScheduledState.StateName));
        }

        public long EnqueuedCount(string queue)
        {
            return UseConnection(connection =>
            {
                var queueApi = GetQueueApi(connection, queue);
                var counters = queueApi.GetEnqueuedAndFetchedCount(queue);

                return counters.EnqueuedCount ?? 0;
            });
        }

        public long FetchedCount(string queue)
        {
            return UseConnection(connection =>
            {
                var queueApi = GetQueueApi(connection, queue);
                var counters = queueApi.GetEnqueuedAndFetchedCount(queue);

                return counters.FetchedCount ?? 0;
            });
        }

        public long FailedCount()
        {
            return UseConnection(connection => GetNumberOfJobsByStateName(connection, FailedState.StateName));
        }

        public long ProcessingCount()
        {
            return UseConnection(connection => GetNumberOfJobsByStateName(connection, ProcessingState.StateName));
        }

        public long SucceededListCount()
        {
            return UseConnection(connection => GetNumberOfJobsByStateName(connection, SucceededState.StateName));
        }

        public long DeletedListCount()
        {
            return UseConnection(connection => GetNumberOfJobsByStateName(connection, DeletedState.StateName));
        }

        public IDictionary<DateTime, long> SucceededByDatesCount()
        {
            return UseConnection(connection => GetTimelineStats(connection, "succeeded"));
        }

        public IDictionary<DateTime, long> FailedByDatesCount()
        {
            return UseConnection(connection => GetTimelineStats(connection, "failed"));
        }

        public IDictionary<DateTime, long> HourlySucceededJobs()
        {
            return UseConnection(connection => GetHourlyTimelineStats(connection, "succeeded"));
        }

        public IDictionary<DateTime, long> HourlyFailedJobs()
        {
            return UseConnection(connection => GetHourlyTimelineStats(connection, "failed"));
        }

        private T UseConnection<T>(Func<HangfireDbContext, T> action)
        {
            var result = action(_database);
            return result;
        }

        private JobList<EnqueuedJobDto> EnqueuedJobs(HangfireDbContext connection, IEnumerable<int> jobIds)
        {
            List<JobDto> jobs = connection.Job
                .Find(Builders<JobDto>.Filter.In(_ => _.Id, jobIds))
                .ToList();

            Dictionary <int, JobQueueDto> jobIdToJobQueueMap = connection.JobQueue
                .Find(Builders<JobQueueDto>.Filter.In(_ => _.JobId, jobs.Select(job => job.Id))
                      & (Builders<JobQueueDto>.Filter.Not(Builders<JobQueueDto>.Filter.Exists(_ => _.FetchedAt))
                      | Builders<JobQueueDto>.Filter.Eq(_ => _.FetchedAt, null)))
                .ToList().ToDictionary(kv => kv.JobId, kv => kv);

            Dictionary<int, StateDto> jobIdToStateMap = connection.State
                .Find(Builders<StateDto>.Filter.In(_ => _.Id, jobs.Select(job => job.StateId)))
                .ToList().ToDictionary(kv => kv.JobId, kv => kv);

            IEnumerable<JobDto> jobsFiltered = jobs.Where(job => jobIdToJobQueueMap.ContainsKey(job.Id));

            List<JobDetailedDto> joinedJobs = jobsFiltered
                .Select(job =>
                {
                    var state = jobIdToStateMap.ContainsKey(job.Id) ? jobIdToStateMap[job.Id] : null;
                    return new JobDetailedDto
                    {
                        Id = job.Id,
                        InvocationData = job.InvocationData,
                        Arguments = job.Arguments,
                        CreatedAt = job.CreatedAt,
                        ExpireAt = job.ExpireAt,
                        FetchedAt = null,
                        StateId = job.StateId,
                        StateName = job.StateName,
                        StateReason = state != null ? state.Reason : null,
                        StateData = state != null ? state.Data : null
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

        private static JobList<TDto> DeserializeJobs<TDto>(ICollection<JobDetailedDto> jobs, Func<JobDetailedDto, Job, Dictionary<string, string>, TDto> selector)
        {
            var result = new List<KeyValuePair<string, TDto>>(jobs.Count);

            foreach (var job in jobs)
            {
                var stateData = JobHelper.FromJson<Dictionary<string, string>>(job.StateData);
                var dto = selector(job, DeserializeJob(job.InvocationData, job.Arguments), stateData);

                result.Add(new KeyValuePair<string, TDto>(
                    job.Id.ToString(), dto));
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

        private IPersistentJobQueueMonitoringApi GetQueueApi(HangfireDbContext connection, string queueName)
        {
            var provider = _queueProviders.GetProvider(queueName);
            var monitoringApi = provider.GetJobQueueMonitoringApi(connection);

            return monitoringApi;
        }

        private JobList<FetchedJobDto> FetchedJobs(HangfireDbContext connection, IEnumerable<int> jobIds)
        {
            List<JobDto> jobs = connection.Job
                .Find(Builders<JobDto>.Filter.In(_ => _.Id, jobIds))
                .ToList();

            Dictionary<int, JobQueueDto> jobIdToJobQueueMap = connection.JobQueue
                .Find(Builders<JobQueueDto>.Filter.In(_ => _.JobId, jobs.Select(job => job.Id))
                      & Builders<JobQueueDto>.Filter.Exists(_ => _.FetchedAt)
                      & Builders<JobQueueDto>.Filter.Not(Builders<JobQueueDto>.Filter.Eq(_ => _.FetchedAt, null)))
                .ToList().ToDictionary(kv => kv.JobId, kv => kv);

            Dictionary<int, StateDto> jobIdToStateMap = connection.State
                .Find(Builders<StateDto>.Filter.In(_ => _.Id, jobs.Select(job => job.StateId)))
                .ToList().ToDictionary(kv => kv.JobId, kv => kv);

            IEnumerable<JobDto> jobsFiltered = jobs.Where(job => jobIdToJobQueueMap.ContainsKey(job.Id));

            List<JobDetailedDto> joinedJobs = jobsFiltered
                .Select(job =>
                {
                    var state = jobIdToStateMap.ContainsKey(job.Id) ? jobIdToStateMap[job.Id] : null;
                    return new JobDetailedDto
                    {
                        Id = job.Id,
                        InvocationData = job.InvocationData,
                        Arguments = job.Arguments,
                        CreatedAt = job.CreatedAt,
                        ExpireAt = job.ExpireAt,
                        FetchedAt = null,
                        StateId = job.StateId,
                        StateName = job.StateName,
                        StateReason = state != null ? state.Reason : null,
                        StateData = state != null ? state.Data : null
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

        private JobList<TDto> GetJobs<TDto>(HangfireDbContext connection, int @from, int count, string stateName, Func<JobDetailedDto, Job, Dictionary<string, string>, TDto> selector)
        {
            // only retrieve job ids
            var jobIds = connection.Job
                .Find(new BsonDocument())
                .Project(_ => new { _.Id, _.StateId })
                .ToList();

            var states = connection.State
                .Find(Builders<StateDto>.Filter.In(_ => _.Id, jobIds.Select(j => j.StateId))
                        & Builders<StateDto>.Filter.Eq(_ => _.Name, stateName))
                .SortByDescending(_ => _.CreatedAt)
                .Skip(@from)
                .Limit(count)
                .Project(_ => new { _.JobId, _.Reason, _.Data })
                .ToList();

            var jobIdToStateMap = states.ToDictionary(kv => kv.JobId, kv => kv);

            // find jobs from states
            List<JobDto> jobsFiltered = connection.Job
                .Find(Builders<JobDto>.Filter.In(_ => _.Id, states.Select(j => j.JobId)))
                .ToList();

            List<JobDetailedDto> joinedJobs = jobsFiltered
                .Select(job =>
                {
                    var state = jobIdToStateMap[job.Id];

                    return new JobDetailedDto
                    {
                        Id = job.Id,
                        InvocationData = job.InvocationData,
                        Arguments = job.Arguments,
                        CreatedAt = job.CreatedAt,
                        ExpireAt = job.ExpireAt,
                        FetchedAt = null,
                        StateId = job.StateId,
                        StateName = job.StateName,
                        StateReason = state != null ? state.Reason : null,
                        StateData = state != null ? state.Data : null
                    };
                })
                .ToList();

            return DeserializeJobs(joinedJobs, selector);
        }

        private long GetNumberOfJobsByStateName(HangfireDbContext connection, string stateName)
        {
            var count = connection.Job.Count(Builders<JobDto>.Filter.Eq(_ => _.StateName, stateName));
            return count;
        }

        private Dictionary<DateTime, long> GetTimelineStats(HangfireDbContext connection, string type)
        {
            var endDate = connection.GetServerTimeUtc().Date;
            var startDate = endDate.AddDays(-7);
            var dates = new List<DateTime>();

            while (startDate <= endDate)
            {
                dates.Add(endDate);
                endDate = endDate.AddDays(-1);
            }

            var stringDates = dates.Select(x => x.ToString("yyyy-MM-dd")).ToList();
            var keys = stringDates.Select(x => String.Format("stats:{0}:{1}", type, x)).ToList();

            var valuesMap = connection.AggregatedCounter
                .Find(Builders<AggregatedCounterDto>.Filter.In(_ => _.Key, keys))
                .ToList()
                .GroupBy(x => x.Key)
                .ToDictionary(x => x.Key, x => (long)x.Count());

            foreach (var key in keys)
            {
                if (!valuesMap.ContainsKey(key)) valuesMap.Add(key, 0);
            }

            var result = new Dictionary<DateTime, long>();
            for (var i = 0; i < stringDates.Count; i++)
            {
                var value = valuesMap[valuesMap.Keys.ElementAt(i)];
                result.Add(dates[i], value);
            }

            return result;
        }

        private Dictionary<DateTime, long> GetHourlyTimelineStats(HangfireDbContext connection, string type)
        {
            var endDate = connection.GetServerTimeUtc();
            var dates = new List<DateTime>();
            for (var i = 0; i < 24; i++)
            {
                dates.Add(endDate);
                endDate = endDate.AddHours(-1);
            }

            var keys = dates.Select(x => String.Format("stats:{0}:{1}", type, x.ToString("yyyy-MM-dd-HH"))).ToList();

            var valuesMap = connection.Counter.Find(Builders<CounterDto>.Filter.In(_ => _.Key, keys))
                .ToList()
                .GroupBy(x => x.Key, x => x)
                .ToDictionary(x => x.Key, x => (long)x.Count());

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