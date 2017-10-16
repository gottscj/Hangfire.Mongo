using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using Hangfire.Mongo.Database;
using Hangfire.Mongo.DistributedLock;
using Hangfire.Mongo.Dto;
using Hangfire.Mongo.PersistentJobQueue;
using Hangfire.States;
using Hangfire.Storage;
using MongoDB.Bson;
using MongoDB.Driver;

namespace Hangfire.Mongo
{
#pragma warning disable 1591

    public sealed class MongoWriteOnlyTransaction : JobStorageTransaction
    {
        private readonly Queue<Action<HangfireDbContext>> _commandQueue = new Queue<Action<HangfireDbContext>>();

        private readonly HangfireDbContext _database;

        private readonly PersistentJobQueueProviderCollection _queueProviders;

        private readonly MongoStorageOptions _storageOptions;

        public MongoWriteOnlyTransaction(HangfireDbContext database,
            PersistentJobQueueProviderCollection queueProviders, MongoStorageOptions storageOptions)
        {
            _database = database ?? throw new ArgumentNullException(nameof(database));
            _queueProviders = queueProviders ?? throw new ArgumentNullException(nameof(queueProviders));
            _storageOptions = storageOptions ?? throw new ArgumentNullException(nameof(storageOptions));
        }

        public override void Dispose()
        {
        }

        public override void ExpireJob(string jobId, TimeSpan expireIn)
        {
            DebugWriteline();
            QueueCommand(x => x.Job.UpdateMany(Builders<JobDto>.Filter.Eq(_ => _.Id, jobId),
                Builders<JobDto>.Update.Set(_ => _.ExpireAt, DateTime.UtcNow.Add(expireIn))));
        }

        public override void PersistJob(string jobId)
        {
            DebugWriteline();
            QueueCommand(x => x.Job.UpdateMany(Builders<JobDto>.Filter.Eq(_ => _.Id, jobId),
                Builders<JobDto>.Update.Set(_ => _.ExpireAt, null)));
        }

        public override void SetJobState(string jobId, IState state)
        {
            DebugWriteline();
            QueueCommand(x =>
            {
                var update = Builders<JobDto>
                    .Update
                    .Set(j => j.StateName, state.Name)
                    .Push(j => j.StateHistory, new StateDto
                    {
                        Name = state.Name,
                        Reason = state.Reason,
                        CreatedAt = DateTime.UtcNow,
                        Data = state.SerializeData()
                    });

                x.Job.UpdateOne(j => j.Id == jobId, update);
            });
        }

        public override void AddJobState(string jobId, IState state)
        {
            DebugWriteline();
            QueueCommand(x =>
            {
                var update = Builders<JobDto>.Update
                    .Push(j => j.StateHistory, new StateDto
                    {
                        Name = state.Name,
                        Reason = state.Reason,
                        CreatedAt = DateTime.UtcNow,
                        Data = state.SerializeData()
                    });

                x.Job.UpdateOne(j => j.Id == jobId, update);
            });
        }

        public override void AddToQueue(string queue, string jobId)
        {
            DebugWriteline();
            IPersistentJobQueueProvider provider = _queueProviders.GetProvider(queue);
            IPersistentJobQueue persistentQueue = provider.GetJobQueue(_database);

            QueueCommand(_ =>
            {
                persistentQueue.Enqueue(queue, jobId);
            });
        }

        public override void IncrementCounter(string key)
        {
            DebugWriteline();
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            QueueCommand(x => x.StateData.InsertOne(new CounterDto
            {
                Id = ObjectId.GenerateNewId(),
                Key = key,
                Value = +1L
            }));
        }

        public override void IncrementCounter(string key, TimeSpan expireIn)
        {
            DebugWriteline();
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            QueueCommand(x => x.StateData.InsertOne(new CounterDto
            {
                Id = ObjectId.GenerateNewId(),
                Key = key,
                Value = +1L,
                ExpireAt = DateTime.UtcNow.Add(expireIn)
            }));
        }

        public override void DecrementCounter(string key)
        {
            DebugWriteline();
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            QueueCommand(x => x.StateData.InsertOne(new CounterDto
            {
                Id = ObjectId.GenerateNewId(),
                Key = key,
                Value = -1L
            }));
        }

        public override void DecrementCounter(string key, TimeSpan expireIn)
        {
            DebugWriteline();
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            QueueCommand(x => x.StateData.InsertOne(new CounterDto
            {
                Id = ObjectId.GenerateNewId(),
                Key = key,
                Value = -1L,
                ExpireAt = DateTime.UtcNow.Add(expireIn)
            }));
        }

        public override void AddToSet(string key, string value)
        {
            DebugWriteline();
            AddToSet(key, value, 0.0);
        }

        public override void AddToSet(string key, string value, double score)
        {
            DebugWriteline();
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            var builder = Builders<SetDto>.Update;
            var set = builder.Set(_ => _.Score, score);
            var setTypesOnInsert = builder.SetOnInsert("_t", new[] { nameof(KeyValueDto), nameof(ExpiringKeyValueDto), nameof(SetDto) });
            var setExpireAt = builder.SetOnInsert(_ => _.ExpireAt, null);
            var update = builder.Combine(set, setTypesOnInsert, setExpireAt);

            QueueCommand(x => x.StateData
                .OfType<SetDto>()
                .UpdateOne(
                    Builders<SetDto>.Filter.Eq(_ => _.Key, key) & Builders<SetDto>.Filter.Eq(_ => _.Value, value),
                    update,
                    new UpdateOptions
                    {
                        IsUpsert = true
                    }));
        }

        public override void RemoveFromSet(string key, string value)
        {
            DebugWriteline();
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            QueueCommand(x => x.StateData
                .OfType<SetDto>()
                .DeleteMany(
                    Builders<SetDto>.Filter.Eq(_ => _.Key, key) &
                    Builders<SetDto>.Filter.Eq(_ => _.Value, value)));
        }

        public override void InsertToList(string key, string value)
        {
            DebugWriteline();
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            QueueCommand(x => x.StateData.InsertOne(new ListDto
            {
                Id = ObjectId.GenerateNewId(),
                Key = key,
                Value = value
            }));
        }

        public override void RemoveFromList(string key, string value)
        {
            DebugWriteline();
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            QueueCommand(x => x.StateData
                .OfType<ListDto>()
                .DeleteMany(
                    Builders<ListDto>.Filter.Eq(_ => _.Key, key) &
                    Builders<ListDto>.Filter.Eq(_ => _.Value, value)));
        }

        public override void TrimList(string key, int keepStartingFrom, int keepEndingAt)
        {
            DebugWriteline();
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            QueueCommand(x =>
            {
                var items = x.StateData
                    .OfType<ListDto>()
                    .Find(new BsonDocument())
                    .Project<KeyValueDto>(Builders<ListDto>.Projection.Include(_ => _.Id))
                    .ToEnumerable()
                    .Where((data, i) => (i < keepStartingFrom) || (i > keepEndingAt))
                    .Select(data => data.Id)
                    .Reverse()
                    .ToArray();

                if (!items.Any())
                {
                    return;
                }

                x.StateData
                    .OfType<ListDto>()
                    .DeleteMany(Builders<ListDto>.Filter.Eq(_ => _.Key, key) &
                                Builders<ListDto>.Filter.In(_ => _.Id, items));
            });
        }

        public override void SetRangeInHash(string key, IEnumerable<KeyValuePair<string, string>> keyValuePairs)
        {
            DebugWriteline();
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            if (keyValuePairs == null)
            {
                throw new ArgumentNullException(nameof(keyValuePairs));
            }

            var builder = Builders<HashDto>.Update;
            var setTypesOnInsert = builder.SetOnInsert("_t", new[] { nameof(KeyValueDto), nameof(ExpiringKeyValueDto), nameof(HashDto) });
            var setExpireAt = builder.SetOnInsert(_ => _.ExpireAt, null);

            foreach (var keyValuePair in keyValuePairs)
            {
                var field = keyValuePair.Key;
                var value = keyValuePair.Value;

                QueueCommand(x =>
                {
                    var set = builder.Set(_ => _.Value, value);
                    var update = builder.Combine(set, setTypesOnInsert, setExpireAt);
                    x.StateData.OfType<HashDto>()
                        .UpdateMany(
                            Builders<HashDto>.Filter.Eq(_ => _.Key, key) &
                            Builders<HashDto>.Filter.Eq(_ => _.Field, field),
                            update,
                            new UpdateOptions
                            {
                                IsUpsert = true
                            });
                });
            }
        }

        public override void RemoveHash(string key)
        {
            DebugWriteline();
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            QueueCommand(x => x.StateData.OfType<HashDto>().DeleteMany(Builders<HashDto>.Filter.Eq(_ => _.Key, key)));
        }

        public override void Commit()
        {
            DebugWriteline();
            //using (new MongoDistributedLock("WriteOnlyTransaction", TimeSpan.FromSeconds(30), _database, _storageOptions))
            {
                foreach (var item in _calls)
                {
                    Debug.WriteLine(item);
                }
                foreach (var action in _commandQueue)
                {
                    action.Invoke(_database);
                }
            }
        }

        private void QueueCommand(Action<HangfireDbContext> action)
        {
            _commandQueue.Enqueue(action);
        }


        // New methods to support Hangfire pro feature - batches.


        public override void ExpireSet(string key, TimeSpan expireIn)
        {
            DebugWriteline();
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            QueueCommand(x => x
                .StateData
                .OfType<SetDto>()
                .UpdateMany(Builders<SetDto>.Filter.Eq(_ => _.Key, key),
                Builders<SetDto>.Update.Set(_ => _.ExpireAt, DateTime.UtcNow.Add(expireIn))));
        }

        public override void ExpireList(string key, TimeSpan expireIn)
        {
            DebugWriteline();
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            QueueCommand(x => x.StateData
                .OfType<ListDto>()
                .UpdateMany(Builders<ListDto>.Filter.Eq(_ => _.Key, key),
                Builders<ListDto>.Update.Set(_ => _.ExpireAt, DateTime.UtcNow.Add(expireIn))));
        }

        public override void ExpireHash(string key, TimeSpan expireIn)
        {
            DebugWriteline();
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            QueueCommand(x => x.StateData
                .OfType<HashDto>()
                .UpdateMany(Builders<HashDto>.Filter.Eq(_ => _.Key, key),
                Builders<HashDto>.Update.Set(_ => _.ExpireAt, DateTime.UtcNow.Add(expireIn))));
        }

        public override void PersistSet(string key)
        {
            DebugWriteline();
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            QueueCommand(x => x.StateData
                .OfType<SetDto>()
                .UpdateMany(Builders<SetDto>.Filter.Eq(_ => _.Key, key),
                    Builders<SetDto>.Update.Set(_ => _.ExpireAt, null)));
        }

        public override void PersistList(string key)
        {
            DebugWriteline();
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            QueueCommand(x => x.StateData
                .OfType<ListDto>()
                .UpdateMany(Builders<ListDto>.Filter.Eq(_ => _.Key, key),
                    Builders<ListDto>.Update.Set(_ => _.ExpireAt, null)));
        }

        public override void PersistHash(string key)
        {
            DebugWriteline();
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            QueueCommand(x => x.StateData
                .OfType<HashDto>()
                .UpdateMany(Builders<HashDto>.Filter.Eq(_ => _.Key, key),
                    Builders<HashDto>.Update.Set(_ => _.ExpireAt, null)));
        }

        public override void AddRangeToSet(string key, IList<string> items)
        {
            DebugWriteline();
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }
            if (items == null)
            {
                throw new ArgumentNullException(nameof(items));
            }
            var builder = Builders<SetDto>.Update;


            var setTypesOnInsert = builder.SetOnInsert("_t", new[] { nameof(KeyValueDto), nameof(ExpiringKeyValueDto), nameof(SetDto) });
            var setExpireAt = builder.SetOnInsert(_ => _.ExpireAt, null);
            var set = builder.Set(_ => _.Score, 0.0);
            var update = builder.Combine(set, setTypesOnInsert, setExpireAt);

            foreach (var item in items)
            {
                QueueCommand(x =>
                {
                    x.StateData
                        .OfType<SetDto>()
                        .UpdateMany(
                            Builders<SetDto>.Filter.Eq(_ => _.Key, key) &
                            Builders<SetDto>.Filter.Eq(_ => _.Value, item),
                            update,
                            new UpdateOptions
                            {
                                IsUpsert = true
                            });
                });
            }

        }

        public override void RemoveSet(string key)
        {
            DebugWriteline();
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }
            QueueCommand(x => x.StateData
                .OfType<SetDto>()
                .DeleteMany(Builders<SetDto>.Filter.Eq(_ => _.Key, key)));
        }
        private List<string> _calls = new List<string>();
        private void DebugWriteline([CallerMemberName] string member = null, params object[] args)
        {
            Debug.WriteLine(DateTime.Now.ToString("hh:mm:ss:fff") + " " + member + $"args: {string.Join(",", args.Select(a => a.ToString()))}");
            // _calls.Add(member);    
        }
    }


#pragma warning restore 1591
}