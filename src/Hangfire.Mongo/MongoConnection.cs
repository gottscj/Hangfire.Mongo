using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Hangfire.Common;
using Hangfire.Mongo.Database;
using Hangfire.Mongo.DistributedLock;
using Hangfire.Mongo.Dto;
using Hangfire.Mongo.PersistentJobQueue;
using Hangfire.Server;
using Hangfire.Storage;
using MongoDB.Bson;
using MongoDB.Driver;

namespace Hangfire.Mongo
{
    /// <summary>
    /// MongoDB database connection for Hangfire
    /// </summary>
    public class MongoConnection : JobStorageConnection
    {
        private readonly MongoStorageOptions _storageOptions;

        private readonly PersistentJobQueueProviderCollection _queueProviders;

        /// <summary>
        /// Ctor using default storage options
        /// </summary>
        public MongoConnection(HangfireDbContext database, PersistentJobQueueProviderCollection queueProviders)
            : this(database, new MongoStorageOptions(), queueProviders)
        {
        }

#pragma warning disable 1591
        public MongoConnection(
            HangfireDbContext database,
            MongoStorageOptions storageOptions,
            PersistentJobQueueProviderCollection queueProviders)
        {
            Database = database ?? throw new ArgumentNullException(nameof(database));
            _storageOptions = storageOptions ?? throw new ArgumentNullException(nameof(storageOptions));
            _queueProviders = queueProviders ?? throw new ArgumentNullException(nameof(queueProviders));
        }

        public HangfireDbContext Database { get; }

        public override IWriteOnlyTransaction CreateWriteTransaction()
        {
            return new MongoWriteOnlyTransaction(Database, _queueProviders, _storageOptions);
        }

        public override IDisposable AcquireDistributedLock(string resource, TimeSpan timeout)
        {
            return new MongoDistributedLock($"Hangfire:{resource}", timeout, Database, _storageOptions);
        }

        public override string CreateExpiredJob(Job job, IDictionary<string, string> parameters, DateTime createdAt, TimeSpan expireIn)
        {
            if (job == null)
            {
                throw new ArgumentNullException(nameof(job));
            }
            if (parameters == null)
            {
                throw new ArgumentNullException(nameof(parameters));
            }

            var invocationData = InvocationData.Serialize(job);

            var jobDto = new JobDto
            {
                InvocationData = JobHelper.ToJson(invocationData),
                Arguments = invocationData.Arguments,
                Parameters = parameters.ToDictionary(kv => kv.Key, kv => kv.Value),
                CreatedAt = createdAt,
                ExpireAt = createdAt.Add(expireIn)
            };

            Database.Job.InsertOne(jobDto);

            var jobId = jobDto.Id;

            return jobId;
        }

        public override IFetchedJob FetchNextJob(string[] queues, CancellationToken cancellationToken)
        {
            if (queues == null || queues.Length == 0)
            {
                throw new ArgumentNullException(nameof(queues));
            }

            var providers = queues
                .Select(queue => _queueProviders.GetProvider(queue))
                .Distinct()
                .ToArray();

            if (providers.Length != 1)
            {
                throw new InvalidOperationException(
                    $"Multiple provider instances registered for queues: {string.Join(", ", queues)}. You should choose only one type of persistent queues per server instance.");
            }

            var persistentQueue = providers[0].GetJobQueue(Database);
            return persistentQueue.Dequeue(queues, cancellationToken);
        }

        public override void SetJobParameter(string id, string name, string value)
        {
            if (id == null)
                throw new ArgumentNullException(nameof(id));

            if (name == null)
                throw new ArgumentNullException(nameof(name));

            var filter = new BsonDocument("_id", id);
            BsonValue bsonValue;
            if (value == null)
            {
                bsonValue = BsonNull.Value;
            }
            else
            {
                bsonValue = value;
            }


            var update = new BsonDocument("$set", new BsonDocument($"{nameof(JobDto.Parameters)}.{name}", bsonValue));

            Database.Job.FindOneAndUpdate(filter, update);
        }

        public override string GetJobParameter(string id, string name)
        {
            if (id == null)
                throw new ArgumentNullException(nameof(id));

            if (name == null)
                throw new ArgumentNullException(nameof(name));

            var parameters = Database
                .Job
                .Find(j => j.Id == id)
                .Project(job => job.Parameters)
                .FirstOrDefault();

            string value = null;
            parameters?.TryGetValue(name, out value);

            return value;
        }

        public override JobData GetJobData(string jobId)
        {
            if (jobId == null)
                throw new ArgumentNullException(nameof(jobId));

            var jobData = Database
                .Job
                .Find(Builders<JobDto>.Filter.Eq(_ => _.Id, jobId))
                .FirstOrDefault();

            if (jobData == null)
                return null;

            // TODO: conversion exception could be thrown.
            var invocationData = JobHelper.FromJson<InvocationData>(jobData.InvocationData);
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
                throw new ArgumentNullException(nameof(jobId));

            var projection = Builders<JobDto>
                .Projection
                .Include(j => j.StateHistory)
                .Slice(j => j.StateHistory, -1);

            var latest = Database
                .Job
                .Find(j => j.Id == jobId)
                .Project(projection)
                .FirstOrDefault();

            if (latest == null)
                return null;

            var state = latest[nameof(JobDto.StateHistory)]
                .AsBsonArray
                .FirstOrDefault();

            if (state == null)
                return null;

            return new StateData
            {
                Name = state[nameof(StateDto.Name)] == BsonNull.Value ? null : state[nameof(StateDto.Name)].AsString,
                Reason = state[nameof(StateDto.Reason)] == BsonNull.Value ? null : state[nameof(StateDto.Reason)].AsString,
                Data = state[nameof(StateDto.Data)] == BsonNull.Value ? new Dictionary<string, string>() :
                    state[nameof(StateDto.Data)].AsBsonDocument.Elements.ToDictionary(e => e.Name, e => e.Value.AsString)
            };
        }

        public override void AnnounceServer(string serverId, ServerContext context)
        {
            if (serverId == null)
                throw new ArgumentNullException(nameof(serverId));

            if (context == null)
                throw new ArgumentNullException(nameof(context));

            var data = new ServerDataDto
            {
                WorkerCount = context.WorkerCount,
                Queues = context.Queues,
                StartedAt = DateTime.UtcNow
            };

            Database.Server.UpdateMany(Builders<ServerDto>.Filter.Eq(_ => _.Id, serverId),
                Builders<ServerDto>.Update.Combine(Builders<ServerDto>.Update.Set(_ => _.Data, JobHelper.ToJson(data)),
                    Builders<ServerDto>.Update.Set(_ => _.LastHeartbeat, DateTime.UtcNow)),
                new UpdateOptions { IsUpsert = true });
        }

        public override void RemoveServer(string serverId)
        {
            if (serverId == null)
            {
                throw new ArgumentNullException(nameof(serverId));
            }

            Database.Server.DeleteMany(Builders<ServerDto>.Filter.Eq(_ => _.Id, serverId));
        }

        public override void Heartbeat(string serverId)
        {
            if (serverId == null)
            {
                throw new ArgumentNullException(nameof(serverId));
            }

            Database.Server.UpdateMany(Builders<ServerDto>.Filter.Eq(_ => _.Id, serverId),
                Builders<ServerDto>.Update.Set(_ => _.LastHeartbeat, DateTime.UtcNow));
        }

        public override int RemoveTimedOutServers(TimeSpan timeOut)
        {
            if (timeOut.Duration() != timeOut)
            {
                throw new ArgumentException("The `timeOut` value must be positive.", nameof(timeOut));
            }

            return (int)Database
                .Server
                .DeleteMany(Builders<ServerDto>.Filter.Lt(_ => _.LastHeartbeat, DateTime.UtcNow.Add(timeOut.Negate())))
                .DeletedCount;
        }

        public override HashSet<string> GetAllItemsFromSet(string key)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            var result = Database
                .StateData
                .OfType<SetDto>()
                .Find(Builders<SetDto>.Filter.Eq(_ => _.Key, key))
                .SortBy(_ => _.Id)
                .Project(_ => _.Value)
                .ToList();

            return new HashSet<string>(result.Cast<string>());
        }

        public override string GetFirstByLowestScoreFromSet(string key, double fromScore, double toScore)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            if (toScore < fromScore)
            {
                throw new ArgumentException("The `toScore` value must be higher or equal to the `fromScore` value.");
            }

            return Database
                .StateData
                .OfType<SetDto>()
                .Find(Builders<SetDto>.Filter.Eq(_ => _.Key, key) &
                      Builders<SetDto>.Filter.Gte(_ => _.Score, fromScore) &
                      Builders<SetDto>.Filter.Lte(_ => _.Score, toScore))
                .SortBy(_ => _.Score)
                .Project(_ => _.Value)
                .FirstOrDefault() as string;
        }

        public override void SetRangeInHash(string key, IEnumerable<KeyValuePair<string, string>> keyValuePairs)
        {
            using (var transaction = new MongoWriteOnlyTransaction(Database, _queueProviders, _storageOptions))
            {
                transaction.SetRangeInHash(key, keyValuePairs);
                transaction.Commit();
            }
        }

        public override Dictionary<string, string> GetAllEntriesFromHash(string key)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            var result = Database
                .StateData
                .OfType<HashDto>()
                .Find(Builders<HashDto>.Filter.Eq(_ => _.Key, key))
                .ToList()
                .ToDictionary(x => x.Field, x => (string)x.Value);

            return result.Count != 0 ? result : null;
        }

        public override long GetSetCount(string key)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            return Database
                .StateData
                .OfType<SetDto>()
                .Find(Builders<SetDto>.Filter.Eq(_ => _.Key, key))
                .Count();
        }

        public override List<string> GetRangeFromSet(string key, int startingFrom, int endingAt)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            return Database
                .StateData
                .OfType<SetDto>()
                .Find(Builders<SetDto>.Filter.Eq(_ => _.Key, key))
                .SortBy(_ => _.Id)
                .Skip(startingFrom)
                .Limit(endingAt - startingFrom + 1) // inclusive -- ensure the last element is included
                .Project(dto => (string)dto.Value)
                .ToList();
        }

        public override TimeSpan GetSetTtl(string key)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            var values = Database
                .StateData
                .OfType<SetDto>()
                .Find(Builders<SetDto>.Filter.Eq(_ => _.Key, key) &
                      Builders<SetDto>.Filter.Not(Builders<SetDto>.Filter.Eq(_ => _.ExpireAt, null)))
                .Project(dto => dto.ExpireAt.Value)
                .ToList();

            return values.Any() ? values.Min() - DateTime.UtcNow : TimeSpan.FromSeconds(-1);
        }

        public override long GetCounter(string key)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            var counterQuery = Database
                .StateData
                .OfType<CounterDto>()
                .Find(Builders<CounterDto>.Filter.Eq(_ => _.Key, key))
                .Project(_ => _.Value)
                .ToList();

            var aggregatedCounterQuery = Database
                .StateData
                .OfType<AggregatedCounterDto>()
                .Find(Builders<AggregatedCounterDto>.Filter.Eq(_ => _.Key, key))
                .Project(_ => _.Value)
                .ToList();

            var values = counterQuery
                .Concat(aggregatedCounterQuery)
                .Select(c => (long)c)
                .ToArray();

            return values.Any() ? values.Sum() : 0;
        }

        public override long GetHashCount(string key)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            return Database
                .StateData
                .OfType<HashDto>()
                .Find(Builders<HashDto>.Filter.Eq(_ => _.Key, key))
                .Count();
        }

        public override TimeSpan GetHashTtl(string key)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            var result = Database
                .StateData
                .OfType<HashDto>()
                .Find(Builders<HashDto>.Filter.Eq(_ => _.Key, key))
                .SortBy(dto => dto.ExpireAt)
                .Project(_ => _.ExpireAt)
                .FirstOrDefault();

            return result.HasValue ? result.Value - DateTime.UtcNow : TimeSpan.FromSeconds(-1);
        }

        public override string GetValueFromHash(string key, string name)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            if (name == null)
            {
                throw new ArgumentNullException(nameof(name));
            }

            var result = Database
                .StateData
                .OfType<HashDto>()
                .Find(Builders<HashDto>.Filter.Eq(_ => _.Key, key) & Builders<HashDto>.Filter.Eq(_ => _.Field, name))
                .FirstOrDefault();

            return result?.Value as string;
        }

        public override long GetListCount(string key)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            return Database
                .StateData
                .OfType<ListDto>()
                .Find(Builders<ListDto>.Filter.Eq(_ => _.Key, key))
                .Count();
        }

        public override TimeSpan GetListTtl(string key)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            var result = Database
                .StateData
                .OfType<ListDto>()
                .Find(Builders<ListDto>.Filter.Eq(_ => _.Key, key))
                .SortBy(_ => _.ExpireAt)
                .Project(_ => _.ExpireAt)
                .FirstOrDefault();

            return result.HasValue ? result.Value - DateTime.UtcNow : TimeSpan.FromSeconds(-1);
        }

        public override List<string> GetRangeFromList(string key, int startingFrom, int endingAt)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            return Database
                .StateData
                .OfType<ListDto>()
                .Find(Builders<ListDto>.Filter.Eq(_ => _.Key, key))
                .SortByDescending(_ => _.Id)
                .Skip(startingFrom)
                .Limit(endingAt - startingFrom + 1) // inclusive -- ensure the last element is included
                .Project(_ => (string)_.Value)
                .ToList();
        }

        public override List<string> GetAllItemsFromList(string key)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            return Database
                .StateData
                .OfType<ListDto>()
                .Find(Builders<ListDto>.Filter.Eq(_ => _.Key, key))
                .SortByDescending(_ => _.Id)
                .Project(_ => (string)_.Value)
                .ToList();
        }
    }
#pragma warning restore 1591
}