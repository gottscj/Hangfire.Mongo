﻿using System.Linq;
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

            _database.Job.InsertOne(jobDto);

            var jobId = jobDto.Id;

            if (parameters.Count > 0)
            {
                foreach (var parameter in parameters)
                {
                    _database.JobParameter.InsertOne(
                        new JobParameterDto { JobId = jobId, Name = parameter.Key, Value = parameter.Value });
                }
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

            _database.JobParameter
                .UpdateMany(
                    Builders<JobParameterDto>.Filter.Eq(_ => _.JobId, int.Parse(id)) & Builders<JobParameterDto>.Filter.Eq(_ => _.Name, name),
                    Builders<JobParameterDto>.Update.Set(_ => _.Value, value),
                    new UpdateOptions
                    {
                        IsUpsert = true
                    });
        }

        public override string GetJobParameter(string id, string name)
        {
            if (id == null)
                throw new ArgumentNullException("id");

            if (name == null)
                throw new ArgumentNullException("name");

            JobParameterDto jobParameter = _database.JobParameter
                .Find(Builders<JobParameterDto>.Filter.Eq(_ => _.JobId, int.Parse(id)) &
                      Builders<JobParameterDto>.Filter.Eq(_ => _.Name, name)).FirstOrDefault();

            return jobParameter != null ? jobParameter.Value : null;
        }

        public override JobData GetJobData(string jobId)
        {
            if (jobId == null)
                throw new ArgumentNullException("jobId");

            JobDto jobData = _database.Job
                    .Find(Builders<JobDto>.Filter.Eq(_ => _.Id, int.Parse(jobId)))
                    .FirstOrDefault();

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

            JobDto job = _database.Job.Find(Builders<JobDto>.Filter.Eq(_ => _.Id, int.Parse(jobId))).FirstOrDefault();
            if (job == null)
                return null;

            StateDto state = _database.State.Find(Builders<StateDto>.Filter.Eq(_ => _.Id, job.StateId)).FirstOrDefault();
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

            _database.Server.UpdateMany(
                Builders<ServerDto>.Filter.Eq(_ => _.Id, serverId),
                Builders<ServerDto>.Update.Combine(
                    Builders<ServerDto>.Update.Set(_ => _.Data, JobHelper.ToJson(data)),
                    Builders<ServerDto>.Update.Set(_ => _.LastHeartbeat, _database.GetServerTimeUtc())),
                new UpdateOptions { IsUpsert = true });
        }

        public override void RemoveServer(string serverId)
        {
            if (serverId == null)
                throw new ArgumentNullException("serverId");

            _database.Server.DeleteMany(Builders<ServerDto>.Filter.Eq(_ => _.Id, serverId));
        }

        public override void Heartbeat(string serverId)
        {
            if (serverId == null)
                throw new ArgumentNullException("serverId");

            _database.Server.UpdateMany(
                Builders<ServerDto>.Filter.Eq(_ => _.Id, serverId),
                Builders<ServerDto>.Update.Set(_ => _.LastHeartbeat, _database.GetServerTimeUtc()));
        }

        public override int RemoveTimedOutServers(TimeSpan timeOut)
        {
            if (timeOut.Duration() != timeOut)
                throw new ArgumentException("The `timeOut` value must be positive.", "timeOut");

            return (int)_database.Server.DeleteMany(
                    Builders<ServerDto>.Filter.Lt(_ => _.LastHeartbeat,
                    _database.GetServerTimeUtc().Add(timeOut.Negate())))
                    .DeletedCount;
        }

        public override HashSet<string> GetAllItemsFromSet(string key)
        {
            if (key == null) throw new ArgumentNullException("key");

            IEnumerable<string> result = _database.Set
                .Find(Builders<SetDto>.Filter.Eq(_ => _.Key, key))
                .Project(_ => _.Value)
                .ToList();

            return new HashSet<string>(result);
        }

        public override string GetFirstByLowestScoreFromSet(string key, double fromScore, double toScore)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            if (toScore < fromScore)
                throw new ArgumentException("The `toScore` value must be higher or equal to the `fromScore` value.");

            return _database.Set
                .Find(Builders<SetDto>.Filter.Eq(_ => _.Key, key) &
                      Builders<SetDto>.Filter.Gte(_ => _.Score, fromScore) &
                      Builders<SetDto>.Filter.Lte(_ => _.Score, toScore))
                .SortBy(_ => _.Score)
                .Project(_ => _.Value)
                .FirstOrDefault();
        }

        public override void SetRangeInHash(string key, IEnumerable<KeyValuePair<string, string>> keyValuePairs)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            if (keyValuePairs == null)
                throw new ArgumentNullException("keyValuePairs");

            foreach (var keyValuePair in keyValuePairs)
            {
                _database.Hash.UpdateMany(Builders<HashDto>.Filter.Eq(_ => _.Key, key) & Builders<HashDto>.Filter.Eq(_ => _.Field, keyValuePair.Key),
                    Builders<HashDto>.Update.Set(_ => _.Value, keyValuePair.Value),
                    new UpdateOptions { IsUpsert = true });
            }
        }

        public override Dictionary<string, string> GetAllEntriesFromHash(string key)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            Dictionary<string, string> result = _database.Hash
                .Find(Builders<HashDto>.Filter.Eq(_ => _.Key, key))
                .ToList().ToDictionary(x => x.Field, x => x.Value);

            return result.Count != 0 ? result : null;
        }

        public override long GetSetCount(string key)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            return _database.Set
                .Find(Builders<SetDto>.Filter.Eq(_ => _.Key, key))
                .Count();
        }

        public override List<string> GetRangeFromSet(string key, int startingFrom, int endingAt)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            return _database.Set
                .Find(Builders<SetDto>.Filter.Eq(_ => _.Key, key))
                .Skip(startingFrom)
                .Limit(endingAt - startingFrom + 1) // inclusive -- ensure the last element is included
                .Project(dto => dto.Value)
                .ToList();
        }

        public override TimeSpan GetSetTtl(string key)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            var values = _database.Set
                .Find(Builders<SetDto>.Filter.Eq(_ => _.Key, key) & Builders<SetDto>.Filter.Not(Builders<SetDto>.Filter.Eq(_ => _.ExpireAt, null)))
                .Project(dto => dto.ExpireAt.Value)
                .ToList();

            if (values.Any() == false)
                return TimeSpan.FromSeconds(-1);

            return values.Min() - DateTime.UtcNow;
        }

        public override long GetCounter(string key)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            List<long> counterQuery = _database.Counter
                .Find(Builders<CounterDto>.Filter.Eq(_ => _.Key, key))
                .Project(_ => (long)_.Value)
                .ToList();

            List<long> aggregatedCounterQuery = _database.AggregatedCounter
                .Find(Builders<AggregatedCounterDto>.Filter.Eq(_ => _.Key, key))
                .Project(_ => _.Value)
                .ToList();

            long[] values = counterQuery.Concat(aggregatedCounterQuery).ToArray();

            return values.Any() ? values.Sum() : 0;
        }

        public override long GetHashCount(string key)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            return _database.Hash
                .Find(Builders<HashDto>.Filter.Eq(_ => _.Key, key))
                .Count();
        }

        public override TimeSpan GetHashTtl(string key)
        {
            if (key == null) throw new ArgumentNullException("key");

            DateTime? result = _database.Hash
                .Find(Builders<HashDto>.Filter.Eq(_ => _.Key, key))
                .SortBy(dto => dto.ExpireAt)
                .Project(_ => _.ExpireAt)
                .FirstOrDefault();

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

            HashDto result = _database.Hash
                .Find(Builders<HashDto>.Filter.Eq(_ => _.Key, key) & Builders<HashDto>.Filter.Eq(_ => _.Field, name))
                .FirstOrDefault();

            return result != null ? result.Value : null;
        }

        public override long GetListCount(string key)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            return _database.List
                .Find(Builders<ListDto>.Filter.Eq(_ => _.Key, key))
                .Count();
        }

        public override TimeSpan GetListTtl(string key)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            DateTime? result = _database.List
                .Find(Builders<ListDto>.Filter.Eq(_ => _.Key, key))
                .SortBy(_ => _.ExpireAt)
                .Project(_ => _.ExpireAt)
                .FirstOrDefault();

            if (!result.HasValue)
                return TimeSpan.FromSeconds(-1);

            return result.Value - DateTime.UtcNow;
        }

        public override List<string> GetRangeFromList(string key, int startingFrom, int endingAt)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            return _database.List
                .Find(Builders<ListDto>.Filter.Eq(_ => _.Key, key))
                .Skip(startingFrom)
                .Limit(endingAt - startingFrom + 1) // inclusive -- ensure the last element is included
                .Project(_ => _.Value)
                .ToList();
        }

        public override List<string> GetAllItemsFromList(string key)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            return _database.List
                .Find(Builders<ListDto>.Filter.Eq(_ => _.Key, key))
                .Project(_ => _.Value)
                .ToList();
        }
    }
#pragma warning restore 1591
}