using System;
using System.Collections.Generic;
using System.Linq;
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

        private readonly HangfireDbContext _connection;

        private readonly PersistentJobQueueProviderCollection _queueProviders;

        private readonly MongoStorageOptions _options;

        public MongoWriteOnlyTransaction(HangfireDbContext connection,
            PersistentJobQueueProviderCollection queueProviders, MongoStorageOptions options)
        {
            if (connection == null)
                throw new ArgumentNullException(nameof(connection));

            if (queueProviders == null)
                throw new ArgumentNullException(nameof(queueProviders));

            if (options == null)
                throw new ArgumentNullException(nameof(options));

            _connection = connection;
            _queueProviders = queueProviders;
            _options = options;
        }

        public override void Dispose()
        {
        }

        public override void ExpireJob(string jobId, TimeSpan expireIn)
        {
            QueueCommand(x => x.Job.UpdateMany(Builders<JobDto>.Filter.Eq(_ => _.Id, jobId),
                Builders<JobDto>.Update.Set(_ => _.ExpireAt, DateTime.UtcNow.Add(expireIn))));
        }

        public override void PersistJob(string jobId)
        {
            QueueCommand(x => x.Job.UpdateMany(Builders<JobDto>.Filter.Eq(_ => _.Id, jobId),
                Builders<JobDto>.Update.Set(_ => _.ExpireAt, null)));
        }

        public override void SetJobState(string jobId, IState state)
        {
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
            IPersistentJobQueueProvider provider = _queueProviders.GetProvider(queue);
            IPersistentJobQueue persistentQueue = provider.GetJobQueue(_connection);

            QueueCommand(_ =>
            {
                persistentQueue.Enqueue(queue, jobId);
            });
        }

        public override void IncrementCounter(string key)
        {
            QueueCommand(x => x.StateData.InsertOne(new CounterDto
            {
                Id = ObjectId.GenerateNewId(),
                Key = key,
                Value = +1L
            }));
        }

        public override void IncrementCounter(string key, TimeSpan expireIn)
        {
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
            QueueCommand(x => x.StateData.InsertOne(new CounterDto
            {
                Id = ObjectId.GenerateNewId(),
                Key = key,
                Value = -1L
            }));
        }

        public override void DecrementCounter(string key, TimeSpan expireIn)
        {
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
            AddToSet(key, value, 0.0);
        }

        public override void AddToSet(string key, string value, double score)
        {
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
            QueueCommand(x => x.StateData
                .OfType<SetDto>()
                .DeleteMany(
                    Builders<SetDto>.Filter.Eq(_ => _.Key, key) &
                    Builders<SetDto>.Filter.Eq(_ => _.Value, value)));
        }

        public override void InsertToList(string key, string value)
        {
            QueueCommand(x => x.StateData.InsertOne(new ListDto
            {
                Id = ObjectId.GenerateNewId(),
                Key = key,
                Value = value
            }));
        }

        public override void RemoveFromList(string key, string value)
        {
            QueueCommand(x => x.StateData
                .OfType<ListDto>()
                .DeleteMany(
                    Builders<ListDto>.Filter.Eq(_ => _.Key, key) &
                    Builders<ListDto>.Filter.Eq(_ => _.Value, value)));
        }

        public override void TrimList(string key, int keepStartingFrom, int keepEndingAt)
        {
            QueueCommand(x =>
            {
                int start = keepStartingFrom + 1;
                int end = keepEndingAt + 1;

                ObjectId[] items = ((IEnumerable<ListDto>)x.StateData.OfType<ListDto>()
                        .Find(new BsonDocument())
                        .Project(Builders<ListDto>.Projection.Include(_ => _.Key))
                        .Project(_ => _)
                        .ToList())
                    .Reverse()
                    .Select((data, i) => new { Index = i + 1, Data = data.Id })
                    .Where(_ => ((_.Index >= start) && (_.Index <= end)) == false)
                    .Select(_ => _.Data)
                    .ToArray();

                x.StateData
                    .OfType<ListDto>()
                    .DeleteMany(Builders<ListDto>.Filter.Eq(_ => _.Key, key) &
                                Builders<ListDto>.Filter.In(_ => _.Id, items));
            });
        }

        public override void SetRangeInHash(string key, IEnumerable<KeyValuePair<string, string>> keyValuePairs)
        {
            if (key == null)
                throw new ArgumentNullException(nameof(key));

            if (keyValuePairs == null)
                throw new ArgumentNullException(nameof(keyValuePairs));

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
            if (key == null)
                throw new ArgumentNullException(nameof(key));

            QueueCommand(x => x.StateData.OfType<HashDto>().DeleteMany(Builders<HashDto>.Filter.Eq(_ => _.Key, key)));
        }

        public override void Commit()
        {
            foreach (var action in _commandQueue)
            {
                action.Invoke(_connection);
            }
        }

        private void QueueCommand(Action<HangfireDbContext> action)
        {
            _commandQueue.Enqueue(action);
        }



        //New methods to support Hangfire pro feature - batches.




        public override void ExpireSet(string key, TimeSpan expireIn)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            QueueCommand(x => x
                .StateData
                .OfType<SetDto>()
                .UpdateMany(Builders<SetDto>.Filter.Eq(_ => _.Key, key),
                Builders<SetDto>.Update.Set(_ => _.ExpireAt, DateTime.UtcNow.Add(expireIn))));
        }

        public override void ExpireList(string key, TimeSpan expireIn)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            QueueCommand(x => x.StateData
                .OfType<ListDto>()
                .UpdateMany(Builders<ListDto>.Filter.Eq(_ => _.Key, key),
                Builders<ListDto>.Update.Set(_ => _.ExpireAt, DateTime.UtcNow.Add(expireIn))));
        }

        public override void ExpireHash(string key, TimeSpan expireIn)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            QueueCommand(x => x.StateData
                .OfType<HashDto>()
                .UpdateMany(Builders<HashDto>.Filter.Eq(_ => _.Key, key),
                Builders<HashDto>.Update.Set(_ => _.ExpireAt, DateTime.UtcNow.Add(expireIn))));
        }

        public override void PersistSet(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            QueueCommand(x => x.StateData
                .OfType<SetDto>()
                .UpdateMany(Builders<SetDto>.Filter.Eq(_ => _.Key, key),
                    Builders<SetDto>.Update.Set(_ => _.ExpireAt, null)));
        }

        public override void PersistList(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            QueueCommand(x => x.StateData
                .OfType<ListDto>()
                .UpdateMany(Builders<ListDto>.Filter.Eq(_ => _.Key, key),
                    Builders<ListDto>.Update.Set(_ => _.ExpireAt, null)));
        }

        public override void PersistHash(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            QueueCommand(x => x.StateData
                .OfType<HashDto>()
                .UpdateMany(Builders<HashDto>.Filter.Eq(_ => _.Key, key),
                    Builders<HashDto>.Update.Set(_ => _.ExpireAt, null)));
        }

        public override void AddRangeToSet(string key, IList<string> items)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            if (items == null) throw new ArgumentNullException(nameof(items));
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
            if (key == null) throw new ArgumentNullException(nameof(key));
            QueueCommand(x => x.StateData
                .OfType<SetDto>()
                .DeleteMany(Builders<SetDto>.Filter.Eq(_ => _.Key, key)));
        }
    }

#pragma warning restore 1591
}