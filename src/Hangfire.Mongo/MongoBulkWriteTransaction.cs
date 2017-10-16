using System;
using System.Collections.Generic;
using System.Linq;
using Hangfire.Mongo.Database;
using Hangfire.Mongo.Dto;
using Hangfire.Mongo.PersistentJobQueue;
using Hangfire.States;
using Hangfire.Storage;
using MongoDB.Bson;
using MongoDB.Driver;

namespace Hangfire.Mongo
{
    internal class MongoBulkWriteTransaction : JobStorageTransaction
    {
        private readonly HangfireDbContext _database;
        private readonly PersistentJobQueueProviderCollection _queueProviders;
        private readonly MongoStorageOptions _storageOptions;
        private readonly Dictionary<Type, object> _stateWrites = new Dictionary<Type, object>();

        private readonly List<Action> _jobAdds = new List<Action>();

        public MongoBulkWriteTransaction(HangfireDbContext database,
            PersistentJobQueueProviderCollection queueProviders, MongoStorageOptions storageOptions)
        {
            this._database = database;
            this._queueProviders = queueProviders;
            this._storageOptions = storageOptions;
        }

        public override void AddJobState(string jobId, IState state)
        {
            var update = Builders<JobDto>.Update
                    .Push(j => j.StateHistory, new StateDto
                    {
                        Name = state.Name,
                        Reason = state.Reason,
                        CreatedAt = DateTime.UtcNow,
                        Data = state.SerializeData()
                    });
            var filter = Builders<JobDto>.Filter.Eq(_ => _.Id, jobId);
            var model = new UpdateOneModel<JobDto>(filter, update);

            QueueWriteModel(model);
        }

        public override void AddToQueue(string queue, string jobId)
        {
            IPersistentJobQueueProvider provider = _queueProviders.GetProvider(queue);
            IPersistentJobQueue persistentQueue = provider.GetJobQueue(_database);
            _jobAdds.Add(() => persistentQueue.Enqueue(queue, jobId));
        }

        public override void AddToSet(string key, string value)
        {
            AddToSet(key, value, 0.0);
        }

        public override void AddToSet(string key, string value, double score)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            var builder = Builders<SetDto>.Update;
            var set = builder.Set(_ => _.Score, score);
            var setTypesOnInsert = builder.SetOnInsert("_t", new[] { nameof(KeyValueDto), nameof(ExpiringKeyValueDto), nameof(SetDto) });
            var setExpireAt = builder.SetOnInsert(_ => _.ExpireAt, null);
            var update = builder.Combine(set, setTypesOnInsert, setExpireAt);
            var filter = Builders<SetDto>.Filter.Eq(_ => _.Key, key) & Builders<SetDto>.Filter.Eq(_ => _.Value, value);

            var model = new UpdateOneModel<SetDto>(filter, update)
            {
                IsUpsert = true
            };
            QueueWriteModel(model);
        }

        public override void Commit()
        {
            throw new NotImplementedException();
        }

        public override void DecrementCounter(string key)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }
            var model = new InsertOneModel<CounterDto>(new CounterDto
            {
                Id = ObjectId.GenerateNewId(),
                Key = key,
                Value = -1L
            });
            QueueWriteModel(model);
        }

        public override void DecrementCounter(string key, TimeSpan expireIn)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            var model = new InsertOneModel<CounterDto>(new CounterDto
            {
                Id = ObjectId.GenerateNewId(),
                Key = key,
                Value = -1L,
                ExpireAt = DateTime.UtcNow.Add(expireIn)
            });
            QueueWriteModel(model);
        }

        public override void ExpireJob(string jobId, TimeSpan expireIn)
        {
            var model = new UpdateManyModel<JobDto>(Builders<JobDto>.Filter.Eq(_ => _.Id, jobId),
            Builders<JobDto>.Update.Set(_ => _.ExpireAt, DateTime.UtcNow.Add(expireIn)));

            QueueWriteModel(model);
        }

        public override void IncrementCounter(string key)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }
            var model = new InsertOneModel<CounterDto>(new CounterDto
            {
                Id = ObjectId.GenerateNewId(),
                Key = key,
                Value = +1L
            });
            QueueWriteModel(model);
        }

        public override void IncrementCounter(string key, TimeSpan expireIn)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }
            var model = new InsertOneModel<CounterDto>(new CounterDto
            {
                Id = ObjectId.GenerateNewId(),
                Key = key,
                Value = +1L,
                ExpireAt = DateTime.UtcNow.Add(expireIn)
            });
            QueueWriteModel(model);
        }

        public override void InsertToList(string key, string value)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            var model = new InsertOneModel<ListDto>(new ListDto
            {
                Id = ObjectId.GenerateNewId(),
                Key = key,
                Value = value
            });

            QueueWriteModel(model);
        }

        public override void PersistJob(string jobId)
        {
            var model = new UpdateManyModel<JobDto>(Builders<JobDto>.Filter.Eq(_ => _.Id, jobId),
                Builders<JobDto>.Update.Set(_ => _.ExpireAt, null));

            QueueWriteModel(model);
        }

        public override void RemoveFromList(string key, string value)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            var model = new DeleteManyModel<ListDto>(Builders<ListDto>.Filter.Eq(_ => _.Key, key) &
                    Builders<ListDto>.Filter.Eq(_ => _.Value, value));

            QueueWriteModel(model);
        }

        public override void RemoveFromSet(string key, string value)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            var model = new DeleteManyModel<SetDto>(Builders<SetDto>.Filter.Eq(_ => _.Key, key) &
                    Builders<SetDto>.Filter.Eq(_ => _.Value, value));

            QueueWriteModel(model);
        }

        public override void RemoveHash(string key)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }
            var model = new DeleteManyModel<HashDto>(Builders<HashDto>.Filter.Eq(_ => _.Key, key));

            QueueWriteModel(model);
        }

        public override void SetJobState(string jobId, IState state)
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
            var filter = Builders<JobDto>.Filter.Eq(_ => _.Id, jobId);
            var model = new UpdateOneModel<JobDto>(filter, update);
            QueueWriteModel(model);
        }

        public override void SetRangeInHash(string key, IEnumerable<KeyValuePair<string, string>> keyValuePairs)
        {
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

                var set = builder.Set(_ => _.Value, value);
                var update = builder.Combine(set, setTypesOnInsert, setExpireAt);

                var model = new UpdateManyModel<HashDto>(
                            Builders<HashDto>.Filter.Eq(_ => _.Key, key) &
                            Builders<HashDto>.Filter.Eq(_ => _.Field, field),
                            update);

                QueueWriteModel(model);
            }
        }

        public override void TrimList(string key, int keepStartingFrom, int keepEndingAt)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            var items = _database.StateData
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

            var model = new DeleteManyModel<ListDto>(
                Builders<ListDto>.Filter.Eq(_ => _.Key, key) &
                Builders<ListDto>.Filter.In(_ => _.Id, items));

                QueueWriteModel(model);
        }

        private void QueueWriteModel<T>(WriteModel<T> writeModel)
        {
            if (_stateWrites.ContainsKey(typeof(T)))
            {
                _stateWrites[typeof(T)] = new List<WriteModel<T>>();
            }
            var list = _stateWrites[typeof(T)] as List<WriteModel<T>>;

            list.Add(writeModel);
        }
    }
}