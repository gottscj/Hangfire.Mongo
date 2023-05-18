using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Hangfire.Annotations;
using Hangfire.Common;
using Hangfire.Logging;
using Hangfire.Mongo.Database;
using Hangfire.Mongo.Dto;
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
        private static readonly ILog Logger = LogProvider.For<MongoConnection>();
        private readonly MongoStorageOptions _storageOptions;
        private readonly MongoJobFetcher _jobFetcher;

        private readonly HangfireDbContext _dbContext;

#pragma warning disable 1591
        public MongoConnection(
            HangfireDbContext database,
            MongoStorageOptions storageOptions)
        {
            _dbContext = database ?? throw new ArgumentNullException(nameof(database));
            _storageOptions = storageOptions ?? throw new ArgumentNullException(nameof(storageOptions));
            _jobFetcher = _storageOptions.Factory.CreateMongoJobFetcher(database, storageOptions);
        }

        public override IWriteOnlyTransaction CreateWriteTransaction()
        {
            return _storageOptions.Factory.CreateMongoWriteOnlyTransaction(_dbContext, _storageOptions);
        }

        public override IDisposable AcquireDistributedLock(string resource, TimeSpan timeout)
        {
            var distributedLock =
                _storageOptions.Factory.CreateMongoDistributedLock(resource, timeout, _dbContext, _storageOptions);

            return distributedLock.AcquireLock();
        }

        public override string CreateExpiredJob(Job job, IDictionary<string, string> parameters, DateTime createdAt,
            TimeSpan expireIn)
        {
            string jobId;
            using (var transaction = _storageOptions.Factory.CreateMongoWriteOnlyTransaction(_dbContext, _storageOptions))
            {
                jobId = transaction.CreateExpiredJob(job, parameters, createdAt, expireIn);
                transaction.Commit();
            }

            return jobId;
        }

        public override IFetchedJob FetchNextJob(string[] queues, CancellationToken cancellationToken)
        {
            if (queues == null || queues.Length == 0)
            {
                throw new ArgumentNullException(nameof(queues));
            }

            return _jobFetcher.FetchNextJob(queues, cancellationToken);
        }

        public override void SetJobParameter(string id, string name, string value)
        {
            using (var transaction = _storageOptions.Factory.CreateMongoWriteOnlyTransaction(_dbContext, _storageOptions))
            {
                transaction.SetJobParameter(id, name, value);
                transaction.Commit();
            }
        }

        public override string GetJobParameter(string id, string name)
        {
            if (id == null)
            {
                throw new ArgumentNullException(nameof(id));
            }

            if (name == null)
            {
                throw new ArgumentNullException(nameof(name));
            }
            var objectId = ObjectId.Parse(id);
            var job = _dbContext
                .JobGraph
                .Find(new BsonDocument
                {
                    ["_id"] = objectId,
                    ["_t"] = nameof(JobDto)
                })
                .FirstOrDefault();
            var jobDto = new JobDto(job);

            string value = null;
            jobDto.Parameters?.TryGetValue(name, out value);

            return value;
        }

        public override JobData GetJobData(string jobId)
        {
            if (jobId == null)
            {
                throw new ArgumentNullException(nameof(jobId));
            }

            var objectId = ObjectId.Parse(jobId);
            var document = _dbContext
                .JobGraph
                .Find(new BsonDocument
                {
                    ["_id"] = objectId,
                    ["_t"] = nameof(JobDto)
                })
                .FirstOrDefault();

            if (document == null)
            {
                return null;
            }

            // TODO: conversion exception could be thrown.
            var jobDto = new JobDto(document);
            var invocationData = JobHelper.FromJson<InvocationData>(jobDto.InvocationData);
            invocationData.Arguments = jobDto.Arguments;

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
                State = jobDto.StateName,
                CreatedAt = jobDto.CreatedAt,
                LoadException = loadException
            };
        }

        public override StateData GetStateData(string jobId)
        {
            if (jobId == null)
            {
                throw new ArgumentNullException(nameof(jobId));
            }

            var objectId = ObjectId.Parse(jobId);
            var document = _dbContext
                .JobGraph
                .Find(new BsonDocument
                {
                    ["_id"] = objectId,
                    ["_t"] = nameof(JobDto)
                })
                .FirstOrDefault();

            if (document == null)
            {
                return null;
            }

            var jobDto = new JobDto(document);
            var state = jobDto.StateHistory.LastOrDefault();

            if (state == null)
            {
                return null;
            }

            return new StateData
            {
                Name = state.Name,
                Reason = state.Reason,
                Data = state.Data
            };
        }

        public override void AnnounceServer(string serverId, ServerContext context)
        {
            if (serverId == null)
            {
                throw new ArgumentNullException(nameof(serverId));
            }

            if (context == null)
            {
                throw new ArgumentNullException(nameof(context));
            }

            var set = new BsonDocument("$set", new BsonDocument
            {
                [nameof(ServerDto.WorkerCount)] = context.WorkerCount,
                [nameof(ServerDto.Queues)] = new BsonArray(context.Queues),
                [nameof(ServerDto.StartedAt)] = DateTime.UtcNow,
                [nameof(ServerDto.LastHeartbeat)] = DateTime.UtcNow
            });

            var filter = new BsonDocument("_id", serverId);
            _dbContext.Server.UpdateOne(filter, set, new UpdateOptions {IsUpsert = true});
        }

        public override void RemoveServer(string serverId)
        {
            if (serverId == null)
            {
                throw new ArgumentNullException(nameof(serverId));
            }

            _dbContext.Server.DeleteMany(new BsonDocument("_id", serverId));
        }

        public override void Heartbeat(string serverId)
        {
            if (serverId == null)
            {
                throw new ArgumentNullException(nameof(serverId));
            }

            var updateResult = _dbContext.Server.UpdateMany(new BsonDocument("_id", serverId),
                new BsonDocument
                {
                    ["$set"] = new BsonDocument
                    {
                        [nameof(ServerDto.LastHeartbeat)] = DateTime.UtcNow
                    }
                });

            if (updateResult != null && updateResult.IsAcknowledged && updateResult.ModifiedCount == 0)
            {
                throw new BackgroundServerGoneException();
            }
        }

        public override int RemoveTimedOutServers(TimeSpan timeOut)
        {
            if (timeOut.Duration() != timeOut)
            {
                throw new ArgumentException("The `timeOut` value must be positive.", nameof(timeOut));
            }

            return (int)_dbContext
                .Server
                .DeleteMany(new BsonDocument
                {
                    [nameof(ServerDto.LastHeartbeat)] = new BsonDocument
                    {
                        ["$lt"] = DateTime.UtcNow.Add(timeOut.Negate())
                    }
                })
                .DeletedCount;
        }

        public override HashSet<string> GetAllItemsFromSet(string key)
        {
            if (Logger.IsTraceEnabled())
            {
                Logger.Trace($"GetAllItemsFromSet({key})");
            }

            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            var result = _dbContext
                .JobGraph
                .Find(new BsonDocument
                {
                    [nameof(SetDto.SetType)] = key,
                    ["_t"] = nameof(SetDto)
                })
                .Sort(new BsonDocument("_id", 1))
                .Project(new BsonDocument(nameof(SetDto.Value), 1))
                .ToList();

            return new HashSet<string>(result.Select(b => b[nameof(SetDto.Value)].AsString));
        }

        public override string GetFirstByLowestScoreFromSet(string key, double fromScore, double toScore)
        {
            return GetFirstByLowestScoreFromSet(key, fromScore, toScore, 1).FirstOrDefault();
        }

        public override List<string> GetFirstByLowestScoreFromSet(string key, double fromScore, double toScore, int count)
        {
            if (Logger.IsTraceEnabled())
            {
                Logger.Trace($"GetFirstByLowestScoreFromSet({key}, {fromScore}, {toScore}, {count})");
            }

            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            if (toScore < fromScore)
            {
                throw new ArgumentException("The `toScore` value must be higher or equal to the `fromScore` value.");
            }

            var results = _dbContext
                .JobGraph
                .Find(new BsonDocument
                {
                    ["_t"] = nameof(SetDto),
                    [nameof(SetDto.SetType)] = key,
                    [nameof(SetDto.Score)] = new BsonDocument
                    {
                        ["$gte"] = fromScore,
                        ["$lte"] = toScore
                    }
                })
                .Sort(new BsonDocument(nameof(SetDto.Score), 1))
                .Project(new BsonDocument(nameof(SetDto.Value), 1))
                .Limit(count)
                .ToList();

            return results.Select(b => b[nameof(SetDto.Value)].AsString).ToList();
        }

        public override void SetRangeInHash(string key, IEnumerable<KeyValuePair<string, string>> keyValuePairs)
        {
            if (Logger.IsTraceEnabled())
            {
                Logger.Trace($"SetRangeInHash({key})");
            }

            using (var transaction = _storageOptions.Factory.CreateMongoWriteOnlyTransaction(_dbContext, _storageOptions))
            {
                transaction.SetRangeInHash(key, keyValuePairs);
                transaction.Commit();
            }
        }

        public override Dictionary<string, string> GetAllEntriesFromHash(string key)
        {
            if (Logger.IsTraceEnabled())
            {
                Logger.Trace($"GetAllEntriesFromHash({key})");
            }

            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            var hash = _dbContext
                .JobGraph
                .Find(new BsonDocument
                {
                    [nameof(KeyJobDto.Key)] = key,
                    ["_t"] = nameof(HashDto)
                })
                .FirstOrDefault();

            return new HashDto(hash).Fields;
        }

        public override long GetSetCount(string key)
        {
            if (Logger.IsTraceEnabled())
            {
                Logger.Trace($"GetSetCount({key})");
            }

            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            return _dbContext
                .JobGraph
                .Find(new BsonDocument
                {
                    [nameof(SetDto.SetType)] = key,
                    ["_t"] = nameof(SetDto)
                })
                .Count();
        }

        public override List<string> GetRangeFromSet(string key, int startingFrom, int endingAt)
        {
            if (Logger.IsTraceEnabled())
            {
                Logger.Trace($"GetRangeFromSet({key}, {startingFrom}, {endingAt})");
            }

            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            var result = _dbContext
                .JobGraph
                .Find(new BsonDocument
                {
                    [nameof(SetDto.SetType)] = key,
                    ["_t"] = nameof(SetDto)
                })
                .Sort(new BsonDocument("_id", 1))
                .Skip(startingFrom)
                .Limit(endingAt - startingFrom + 1) // inclusive -- ensure the last element is included
                .Project(new BsonDocument(nameof(SetDto.Value), 1))
                .ToList();
            return result.Select(b => b[nameof(SetDto.Value)].AsString).ToList();
        }

        public override TimeSpan GetSetTtl(string key)
        {
            if (Logger.IsTraceEnabled())
            {
                Logger.Trace($"GetSetTtl({key})");
            }

            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            var values = _dbContext
                .JobGraph
                .Find(new BsonDocument
                {
                    [nameof(SetDto.SetType)] = key,
                    ["_t"] = nameof(SetDto),
                    [nameof(SetDto.ExpireAt)] = new BsonDocument
                    {
                        ["$not"] = new BsonDocument("$eq", BsonNull.Value)
                    }
                })
                .Project(new BsonDocument(nameof(SetDto.ExpireAt), 1))
                .ToList()
                .Select(b => b[nameof(SetDto.ExpireAt)].ToUniversalTime())
                .ToList();

            return values.Any() ? values.Min() - DateTime.UtcNow : TimeSpan.FromSeconds(-1);
        }

        public override long GetCounter(string key)
        {
            if (Logger.IsTraceEnabled())
            {
                Logger.Trace($"GetCounter({key})");
            }

            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            var counter = _dbContext
                .JobGraph
                .Find(new BsonDocument
                {
                    [nameof(CounterDto.Key)] = key,
                    ["_t"] = nameof(CounterDto)
                })
                .FirstOrDefault();

            return counter == null ? 0 : new CounterDto(counter).Value;
        }

        public override long GetHashCount(string key)
        {
            if (Logger.IsTraceEnabled())
            {
                Logger.Trace($"GetHashCount({key})");
            }

            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            var hash = _dbContext
                .JobGraph
                .Find(new BsonDocument
                {
                    [nameof(HashDto.Key)] = key,
                    ["_t"] = nameof(HashDto)
                })
                .FirstOrDefault();

            return hash == null ? 0 : new HashDto(hash).Fields.Count;
        }

        public override TimeSpan GetHashTtl(string key)
        {
            if (Logger.IsTraceEnabled())
            {
                Logger.Trace($"GetHashTtl({key})");
            }

            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }
            var hash = _dbContext
               .JobGraph
               .Find(new BsonDocument
               {
                   [nameof(HashDto.Key)] = key,
                   ["_t"] = nameof(HashDto)
               })
               .Sort(new BsonDocument(nameof(HashDto.ExpireAt), 1))
               .Project(new BsonDocument(nameof(HashDto.ExpireAt), 1))
               .FirstOrDefault();

            if(hash == null)
            {
                return TimeSpan.FromSeconds(-1);
            }
            var result = hash[nameof(HashDto.ExpireAt)].ToNullableUniversalTime();

            return result.HasValue ? result.Value - DateTime.UtcNow : TimeSpan.FromSeconds(-1);
        }

        public override string GetValueFromHash(string key, string name)
        {
            if (Logger.IsTraceEnabled())
            {
                Logger.Trace($"GetValueFromHash({key}, {name})");
            }

            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            if (name == null)
            {
                throw new ArgumentNullException(nameof(name));
            }

            var hashWithField = new BsonDocument("$and", new BsonArray
            {
                new BsonDocument("_t", nameof(HashDto)),
                new BsonDocument(nameof(KeyJobDto.Key), key),
                new BsonDocument($"{nameof(HashDto.Fields)}.{name}", new BsonDocument("$exists", true))
            });

            var result = _dbContext
                .JobGraph
                .Find(hashWithField)
                .FirstOrDefault();

            return result == null ? null : new HashDto(result).Fields[name];
        }

        public override long GetListCount(string key)
        {
            if (Logger.IsTraceEnabled())
            {
                Logger.Trace($"GetListCount({key})");
            }

            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            return _dbContext
                .JobGraph
                .Find(new BsonDocument
                {
                    [nameof(ListDto.Item)] = key,
                    ["_t"] = nameof(ListDto)
                })
                .Count();
        }

        public override TimeSpan GetListTtl(string key)
        {
            if (Logger.IsTraceEnabled())
            {
                Logger.Trace($"GetListTtl({key})");
            }

            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            var listDto = _dbContext
                .JobGraph
                .Find(new BsonDocument
                {
                    [nameof(ListDto.Item)] = key,
                    ["_t"] = nameof(ListDto)
                })
                .Sort(new BsonDocument(nameof(ListDto.ExpireAt), 1))
                .Project(new BsonDocument(nameof(ListDto.ExpireAt), 1))
                .FirstOrDefault();

            if(listDto == null)
            {
                return TimeSpan.FromSeconds(-1);
            }
            var result = listDto[nameof(ListDto.ExpireAt)].ToNullableUniversalTime();
            return result.HasValue ? result.Value - DateTime.UtcNow : TimeSpan.FromSeconds(-1);
        }

        public override List<string> GetRangeFromList(string key, int startingFrom, int endingAt)
        {
            if (Logger.IsTraceEnabled())
            {
                Logger.Trace($"GetRangeFromList({key}, {startingFrom}, {endingAt})");
            }

            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            return _dbContext
                .JobGraph.Find(new BsonDocument
                {
                    [nameof(ListDto.Item)] = key,
                    ["_t"] = nameof(ListDto)
                })
                .Sort(new BsonDocument("_id", -1))
                .Skip(startingFrom)
                .Limit(endingAt - startingFrom + 1) // inclusive -- ensure the last element is included
                .Project(new BsonDocument(nameof(ListDto.Value), 1))
                .ToList()
                .Select(b => b[nameof(ListDto.Value)].AsString).ToList();
        }

        public override List<string> GetAllItemsFromList(string key)
        {
            if (Logger.IsTraceEnabled())
            {
                Logger.Trace($"GetAllItemsFromList({key})");
            }

            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }
            return _dbContext
                .JobGraph.Find(new BsonDocument
                {
                    [nameof(ListDto.Item)] = key,
                    ["_t"] = nameof(ListDto)
                })
                .Sort(new BsonDocument("_id", -1))
                .Project(new BsonDocument(nameof(ListDto.Value), 1))
                .ToList()
                .Select(b => b[nameof(ListDto.Value)].AsString).ToList();
        }

        public override DateTime GetUtcDateTime()
        {
            if (Logger.IsTraceEnabled())
            {
                Logger.Trace($"GetUtcDateTime()");
            }

            try
            {
                var pipeline = new[]
                {
                    new BsonDocument("$project", new BsonDocument("date", "$$NOW"))
                };
                // we should always have a schema document in the db, and this 
                var time = _dbContext.Schema.Aggregate<BsonDocument>(pipeline).FirstOrDefault();
                if (time is null)
                {
                    throw new InvalidOperationException("No documents in the schema collection");
                }
                return time["date"].ToUniversalTime();
            }
            catch (Exception e)
            {
                Logger.WarnException("Failed to get UTC datetime from mongodb server, using local UTC", e);
                return DateTime.UtcNow;
            }
        }

        public override bool GetSetContains([NotNull] string key, [NotNull] string value)
        {
            if (Logger.IsTraceEnabled())
            {
                Logger.Trace($"GetSetContains({key}, {value})");
            }

            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            return _dbContext
                .JobGraph
                .Find(new BsonDocument
                {
                    [nameof(SetDto.Key)] = $"{key}<{value}>",
                    ["_t"] = nameof(SetDto)
                })
                .Any();
        }

        public override long GetSetCount([NotNull] IEnumerable<string> keys, int limit)
        {
            if (Logger.IsTraceEnabled())
            {
                // ReSharper disable once PossibleMultipleEnumeration
                Logger.Trace($"GetSetCount({string.Join(",", keys)}, {limit})");
            }

            // ReSharper disable once PossibleMultipleEnumeration
            if (!keys.Any())
            {
                return 0;
            }

            if (keys == null) throw new ArgumentNullException(nameof(keys));
            if (limit < 0) throw new ArgumentOutOfRangeException(nameof(limit), "Value must be greater or equal to 0.");

            return _dbContext
                .JobGraph
                .Find(new BsonDocument
                {
                    [nameof(SetDto.SetType)] = new BsonDocument("$in", BsonArray.Create(keys)),
                    ["_t"] = nameof(SetDto)
                })
                .Limit(limit)
                .Count();
        }
    }
#pragma warning restore 1591
}