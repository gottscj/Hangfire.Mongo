using System.Linq;
using Hangfire.Common;
using Hangfire.Mongo.Database;
using Hangfire.Mongo.DistributedLock;
using Hangfire.Mongo.Dto;
using Hangfire.Mongo.MongoUtils;
using Hangfire.Mongo.PersistentJobQueue;
using Hangfire.Server;
using Hangfire.Storage;
using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Hangfire.Mongo.Helpers;

namespace Hangfire.Mongo
{
#pragma warning disable 1591
    /// <summary>
    /// MongoDB database connection for Hangfire
    /// </summary>
    public class MongoConnection : JobStorageConnection
    {
        private readonly HangfireDbContext _database;
        private readonly MongoStorageOptions _options;

        private readonly PersistentJobQueueProviderCollection _queueProviders;

        public MongoConnection(HangfireDbContext database, PersistentJobQueueProviderCollection queueProviders)
            : this(database, new MongoStorageOptions(), queueProviders)
        {
        }

        public MongoConnection(HangfireDbContext database, MongoStorageOptions options, PersistentJobQueueProviderCollection queueProviders)
        {
            if (database == null)
                throw new ArgumentNullException("database");

            if (queueProviders == null)
                throw new ArgumentNullException("queueProviders");

            if (options == null)
                throw new ArgumentNullException("options");

            _database = database;
            _options = options;
            _queueProviders = queueProviders;
        }

        public HangfireDbContext Database
        {
            get { return _database;  }
        }

        public override IWriteOnlyTransaction CreateWriteTransaction()
        {
            return new MongoWriteOnlyTransaction(_database, _queueProviders);
        }

        public override IDisposable AcquireDistributedLock(string resource, TimeSpan timeout)
        {
            return new MongoDistributedLock(String.Format("HangFire:{0}", resource), timeout, _database, _options);
        }

        public override string CreateExpiredJob(Job job, IDictionary<string, string> parameters, DateTime createdAt, TimeSpan expireIn)
        {
            if (job == null)
                throw new ArgumentNullException("job");

            if (parameters == null)
                throw new ArgumentNullException("parameters");

            var invocationData = InvocationData.Serialize(job);

            var jobDto = new JobDto
            {
                InvocationData = JobHelper.ToJson(invocationData),
                Arguments = invocationData.Arguments,
                CreatedAt = createdAt,
                ExpireAt = createdAt.Add(expireIn)
            };

            AsyncHelper.RunSync(() => _database.Job.InsertOneAsync(jobDto));

            var jobId = jobDto.Id;

            if (parameters.Count > 0)
            {
                Task.WaitAll(parameters
                    .Select(parameter => _database
                        .JobParameter
                        .InsertOneAsync(new JobParameterDto
                        {
                            JobId = jobId,
                            Name = parameter.Key,
                            Value = parameter.Value
                        }))
                    .ToArray());
            }

            return jobId.ToString();
        }

        public override IFetchedJob FetchNextJob(string[] queues, CancellationToken cancellationToken)
        {
            if (queues == null || queues.Length == 0)
                throw new ArgumentNullException("queues");

            var providers = queues
                .Select(queue => _queueProviders.GetProvider(queue))
                .Distinct()
                .ToArray();

            if (providers.Length != 1)
            {
                throw new InvalidOperationException(String.Format("Multiple provider instances registered for queues: {0}. You should choose only one type of persistent queues per server instance.",
                    String.Join(", ", queues)));
            }

            var persistentQueue = providers[0].GetJobQueue(_database);
            return persistentQueue.Dequeue(queues, cancellationToken);
        }

        public override void SetJobParameter(string id, string name, string value)
        {
            if (id == null)
                throw new ArgumentNullException("id");

            if (name == null)
                throw new ArgumentNullException("name");

            AsyncHelper.RunSync(() => _database.JobParameter
                .UpdateManyAsync(
                    Builders<JobParameterDto>.Filter.Eq(_ => _.JobId, int.Parse(id)) & Builders<JobParameterDto>.Filter.Eq(_ => _.Name, name),
                    Builders<JobParameterDto>.Update.Set(_ => _.Value, value),
                    new UpdateOptions
                    {
                        IsUpsert = true
                    }));
        }

        public override string GetJobParameter(string id, string name)
        {
            if (id == null)
                throw new ArgumentNullException("id");

            if (name == null)
                throw new ArgumentNullException("name");

            JobParameterDto jobParameter = AsyncHelper.RunSync(() => _database.JobParameter
                .Find(Builders<JobParameterDto>.Filter.Eq(_ => _.JobId, int.Parse(id)) &
                      Builders<JobParameterDto>.Filter.Eq(_ => _.Name, name)).FirstOrDefaultAsync());

            return jobParameter != null ? jobParameter.Value : null;
        }

        public override JobData GetJobData(string jobId)
        {
            if (jobId == null)
                throw new ArgumentNullException("jobId");

            JobDto jobData = AsyncHelper.RunSync(() => _database.Job
                    .Find(Builders<JobDto>.Filter.Eq(_ => _.Id, int.Parse(jobId)))
                    .FirstOrDefaultAsync());

            if (jobData == null)
                return null;

            // TODO: conversion exception could be thrown.
            InvocationData invocationData = JobHelper.FromJson<InvocationData>(jobData.InvocationData);
            invocationData.Arguments = jobData.Arguments;

            Job job = null;
            JobLoadException loadException = null;

            try
            {
                job = invocationData.Deserialize();
            }
            catch (JobLoadException ex)
            {
                loadException = ex;
            }

            return new JobData
            {
                Job = job,
                State = jobData.StateName,
                CreatedAt = jobData.CreatedAt,
                LoadException = loadException
            };
        }

        public override StateData GetStateData(string jobId)
        {
            if (jobId == null)
                throw new ArgumentNullException("jobId");

            JobDto job = AsyncHelper.RunSync(() => _database.Job.Find(Builders<JobDto>.Filter.Eq(_ => _.Id, int.Parse(jobId))).FirstOrDefaultAsync());
            if (job == null)
                return null;

            StateDto state = AsyncHelper.RunSync(() => _database.State.Find(Builders<StateDto>.Filter.Eq(_ => _.Id, job.StateId)).FirstOrDefaultAsync());
            if (state == null)
                return null;

            return new StateData
            {
                Name = state.Name,
                Reason = state.Reason,
                Data = JobHelper.FromJson<Dictionary<string, string>>(state.Data)
            };
        }

        public override void AnnounceServer(string serverId, ServerContext context)
        {
            if (serverId == null)
                throw new ArgumentNullException("serverId");

            if (context == null)
                throw new ArgumentNullException("context");

            var data = new ServerDataDto
            {
                WorkerCount = context.WorkerCount,
                Queues = context.Queues,
                StartedAt = _database.GetServerTimeUtc(),
            };

            _database.Server.UpdateManyAsync(Builders<ServerDto>.Filter.Eq(_ => _.Id, serverId),
                Builders<ServerDto>.Update.Combine(Builders<ServerDto>.Update.Set(_ => _.Data, JobHelper.ToJson(data)), Builders<ServerDto>.Update.Set(_ => _.LastHeartbeat, _database.GetServerTimeUtc())),
                new UpdateOptions { IsUpsert = true });
        }

        public override void RemoveServer(string serverId)
        {
            if (serverId == null)
                throw new ArgumentNullException("serverId");

            AsyncHelper.RunSync(() => _database.Server.DeleteManyAsync(Builders<ServerDto>.Filter.Eq(_ => _.Id, serverId)));
        }

        public override void Heartbeat(string serverId)
        {
            if (serverId == null)
                throw new ArgumentNullException("serverId");

            AsyncHelper.RunSync(() => _database.Server.UpdateManyAsync(Builders<ServerDto>.Filter.Eq(_ => _.Id, serverId),
                Builders<ServerDto>.Update.Set(_ => _.LastHeartbeat, _database.GetServerTimeUtc())));
        }

        public override int RemoveTimedOutServers(TimeSpan timeOut)
        {
            if (timeOut.Duration() != timeOut)
                throw new ArgumentException("The `timeOut` value must be positive.", "timeOut");

            return (int)AsyncHelper.RunSync(() => _database.Server.DeleteManyAsync(Builders<ServerDto>.Filter.Lt(_ => _.LastHeartbeat, _database.GetServerTimeUtc().Add(timeOut.Negate()))))
                    .DeletedCount;
        }

        public override HashSet<string> GetAllItemsFromSet(string key)
        {
            if (key == null) throw new ArgumentNullException("key");

            IEnumerable<string> result = AsyncHelper.RunSync(() => _database.Set.Find(Builders<SetDto>.Filter.Eq(_ => _.Key, key)).ToListAsync())
                .Select(x => x.Value);

            return new HashSet<string>(result);
        }

        public override string GetFirstByLowestScoreFromSet(string key, double fromScore, double toScore)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            if (toScore < fromScore)
                throw new ArgumentException("The `toScore` value must be higher or equal to the `fromScore` value.");

            SetDto set = AsyncHelper.RunSync(() => _database.Set
                .Find(Builders<SetDto>.Filter.Eq(_ => _.Key, key) &
                      Builders<SetDto>.Filter.Gte(_ => _.Score, fromScore) &
                      Builders<SetDto>.Filter.Lte(_ => _.Score, toScore))
                .Sort(Builders<SetDto>.Sort.Ascending(_ => _.Score))
                .FirstOrDefaultAsync());

            return set != null ? set.Value : null;
        }

        public override void SetRangeInHash(string key, IEnumerable<KeyValuePair<string, string>> keyValuePairs)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            if (keyValuePairs == null)
                throw new ArgumentNullException("keyValuePairs");

            List<Task> tasks = new List<Task>();

            foreach (var keyValuePair in keyValuePairs)
            {
                tasks.Add(_database.Hash.UpdateManyAsync(Builders<HashDto>.Filter.Eq(_ => _.Key, key) & Builders<HashDto>.Filter.Eq(_ => _.Field, keyValuePair.Key),
                    Builders<HashDto>.Update.Set(_ => _.Value, keyValuePair.Value),
                    new UpdateOptions { IsUpsert = true }));
            }

            Task.WaitAll(tasks.ToArray());
        }

        public override Dictionary<string, string> GetAllEntriesFromHash(string key)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            Dictionary<string, string> result = AsyncHelper.RunSync(() => _database.Hash.Find(Builders<HashDto>.Filter.Eq(_ => _.Key, key)).ToListAsync())
                .ToDictionary(x => x.Field, x => x.Value);

            return result.Count != 0 ? result : null;
        }

        public override long GetSetCount(string key)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            return AsyncHelper.RunSync(() => _database.Set.Find(Builders<SetDto>.Filter.Eq(_ => _.Key, key)).CountAsync());
        }

        public override List<string> GetRangeFromSet(string key, int startingFrom, int endingAt)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            return AsyncHelper.RunSync(() => _database.Set
                    .Find(Builders<SetDto>.Filter.Eq(_ => _.Key, key))
                    .ToListAsync())
                .Select((data, i) => new { Index = i + 1, Data = data })
                .Where(_ => (_.Index >= startingFrom + 1) && (_.Index <= endingAt + 1))
                .Select(x => x.Data.Value)
                .ToList();
        }

        public override TimeSpan GetSetTtl(string key)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            DateTime[] values = AsyncHelper.RunSync(() => _database.Set.Find(Builders<SetDto>.Filter.Eq(_ => _.Key, key)).ToListAsync())
                    .Where(_ => _.ExpireAt.HasValue)
                    .Select(_ => _.ExpireAt.Value)
                    .ToArray();

            if (values.Any() == false)
                return TimeSpan.FromSeconds(-1);

            return values.Min() - DateTime.UtcNow;
        }

        public override long GetCounter(string key)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            long[] values = AsyncHelper.RunSync(() => _database.Counter.Find(Builders<CounterDto>.Filter.Eq(_ => _.Key, key)).ToListAsync()).Select(_ => (long)_.Value)
                .Concat(AsyncHelper.RunSync(() => _database.AggregatedCounter.Find(Builders<AggregatedCounterDto>.Filter.Eq(_ => _.Key, key)).ToListAsync()).Select(_ => _.Value))
                .ToArray();

            return values.Any() ? values.Sum() : 0;
        }

        public override long GetHashCount(string key)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            return AsyncHelper.RunSync(() =>
                _database.Hash.Find(Builders<HashDto>.Filter.Eq(_ => _.Key, key)).CountAsync());
        }

        public override TimeSpan GetHashTtl(string key)
        {
            if (key == null) throw new ArgumentNullException("key");

            List<HashDto> hashes = AsyncHelper.RunSync(() => _database.Hash.Find(Builders<HashDto>.Filter.Eq(_ => _.Key, key)).ToListAsync());
            DateTime? result = hashes.Any() ? hashes.Min(x => x.ExpireAt) : null;

            if (!result.HasValue)
                return TimeSpan.FromSeconds(-1);

            return result.Value - DateTime.UtcNow;
        }

        public override string GetValueFromHash(string key, string name)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            if (name == null)
                throw new ArgumentNullException("name");

            HashDto result = AsyncHelper.RunSync(() => _database.Hash
                .Find(Builders<HashDto>.Filter.Eq(_ => _.Key, key) & Builders<HashDto>.Filter.Eq(_ => _.Field, name))
                .FirstOrDefaultAsync());

            return result != null ? result.Value : null;
        }

        public override long GetListCount(string key)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            return AsyncHelper.RunSync(() =>
                _database.List.Find(Builders<ListDto>.Filter.Eq(_ => _.Key, key)).CountAsync());
        }

        public override TimeSpan GetListTtl(string key)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            List<ListDto> items = AsyncHelper.RunSync(() => _database.List.Find(Builders<ListDto>.Filter.Eq(_ => _.Key, key)).ToListAsync());
            DateTime? result = items.Any() ? items.Min(_ => _.ExpireAt) : null;

            if (!result.HasValue)
                return TimeSpan.FromSeconds(-1);

            return result.Value - DateTime.UtcNow;
        }

        public override List<string> GetRangeFromList(string key, int startingFrom, int endingAt)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            return AsyncHelper.RunSync(() => _database.List.Find(Builders<ListDto>.Filter.Eq(_ => _.Key, key)).ToListAsync())
                .Select((data, i) => new { Index = i + 1, Data = data })
                .Where(_ => (_.Index >= startingFrom + 1) && (_.Index <= endingAt + 1))
                .Select(x => x.Data.Value)
                .ToList();
        }

        public override List<string> GetAllItemsFromList(string key)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            return AsyncHelper.RunSync(() => _database.List.Find(Builders<ListDto>.Filter.Eq(_ => _.Key, key)).ToListAsync())
                .Select(_ => _.Value)
                .ToList();
        }
    }
#pragma warning restore 1591
}