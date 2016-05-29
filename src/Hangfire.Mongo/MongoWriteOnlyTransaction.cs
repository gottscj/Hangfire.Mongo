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

        public MongoWriteOnlyTransaction(HangfireDbContext connection, PersistentJobQueueProviderCollection queueProviders)
        {
            if (connection == null)
                throw new ArgumentNullException("connection");

            if (queueProviders == null)
                throw new ArgumentNullException("queueProviders");

            _connection = connection;
            _queueProviders = queueProviders;
        }

        public override void Dispose()
        {
        }

        public override void ExpireJob(string jobId, TimeSpan expireIn)
        {
            QueueCommand(x => x.Job.UpdateMany(Builders<JobDto>.Filter.Eq(_ => _.Id, int.Parse(jobId)),
                Builders<JobDto>.Update.Set(_ => _.ExpireAt, _connection.GetServerTimeUtc().Add(expireIn))));
        }

        public override void PersistJob(string jobId)
        {
            QueueCommand(x => x.Job.UpdateMany(Builders<JobDto>.Filter.Eq(_ => _.Id, int.Parse(jobId)),
                Builders<JobDto>.Update.Set(_ => _.ExpireAt, null)));
        }

        public override void SetJobState(string jobId, IState state)
        {
            QueueCommand(x =>
            {
                StateDto stateDto = new StateDto
                {
                    Id = ObjectId.GenerateNewId(),
                    JobId = int.Parse(jobId),
                    Name = state.Name,
                    Reason = state.Reason,
                    CreatedAt = _connection.GetServerTimeUtc(),
                    Data = JobHelper.ToJson(state.SerializeData())
                };
                x.State.InsertOne(stateDto);

                x.Job.UpdateMany(
                    Builders<JobDto>.Filter.Eq(_ => _.Id, int.Parse(jobId)),
                    Builders<JobDto>.Update.Set(_ => _.StateId, stateDto.Id));

                x.Job.UpdateMany(Builders<JobDto>.Filter.Eq(_ => _.Id, int.Parse(jobId)),
                    Builders<JobDto>.Update.Set(_ => _.StateName, state.Name));
            });
        }

        public override void AddJobState(string jobId, IState state)
        {
            QueueCommand(x => x.State.InsertOne(new StateDto
            {
                Id = ObjectId.GenerateNewId(),
                JobId = int.Parse(jobId),
                Name = state.Name,
                Reason = state.Reason,
                CreatedAt = _connection.GetServerTimeUtc(),
                Data = JobHelper.ToJson(state.SerializeData())
            }));
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
            QueueCommand(x => x.Counter.InsertOne(new CounterDto
            {
                Id = ObjectId.GenerateNewId(),
                Key = key,
                Value = +1
            }));
        }

        public override void IncrementCounter(string key, TimeSpan expireIn)
        {
            QueueCommand(x => x.Counter.InsertOne(new CounterDto
            {
                Id = ObjectId.GenerateNewId(),
                Key = key,
                Value = +1,
                ExpireAt = _connection.GetServerTimeUtc().Add(expireIn)
            }));
        }

        public override void DecrementCounter(string key)
        {
            QueueCommand(x => x.Counter.InsertOne(new CounterDto
            {
                Id = ObjectId.GenerateNewId(),
                Key = key,
                Value = -1
            }));
        }

        public override void DecrementCounter(string key, TimeSpan expireIn)
        {
            QueueCommand(x => x.Counter.InsertOne(new CounterDto
            {
                Id = ObjectId.GenerateNewId(),
                Key = key,
                Value = -1,
                ExpireAt = _connection.GetServerTimeUtc().Add(expireIn)
            }));
        }

        public override void AddToSet(string key, string value)
        {
            AddToSet(key, value, 0.0);
        }

        public override void AddToSet(string key, string value, double score)
        {
            QueueCommand(x => x.Set.UpdateMany(Builders<SetDto>.Filter.Eq(_ => _.Key, key) & Builders<SetDto>.Filter.Eq(_ => _.Value, value),
                Builders<SetDto>.Update.Set(_ => _.Score, score),
                new UpdateOptions
                {
                    IsUpsert = true
                }));
        }

        public override void RemoveFromSet(string key, string value)
        {
            QueueCommand(x => x.Set.DeleteMany(
                Builders<SetDto>.Filter.Eq(_ => _.Key, key) &
                Builders<SetDto>.Filter.Eq(_ => _.Value, value)));
        }

        public override void InsertToList(string key, string value)
        {
            QueueCommand(x => x.List.InsertOne(new ListDto
            {
                Id = ObjectId.GenerateNewId(),
                Key = key,
                Value = value
            }));
        }

        public override void RemoveFromList(string key, string value)
        {
            QueueCommand(x => x.List.DeleteMany(
                Builders<ListDto>.Filter.Eq(_ => _.Key, key) &
                Builders<ListDto>.Filter.Eq(_ => _.Value, value)));
        }

        public override void TrimList(string key, int keepStartingFrom, int keepEndingAt)
        {
            QueueCommand(x =>
            {
                int start = keepStartingFrom + 1;
                int end = keepEndingAt + 1;

                ObjectId[] items = ((IEnumerable<ListDto>)x.List
                        .Find(new BsonDocument())
                        .Project(Builders<ListDto>.Projection.Include(_ => _.Key))
                        .Project(_ => _)
                        .ToList())
                    .Reverse()
                    .Select((data, i) => new { Index = i + 1, Data = data.Id })
                    .Where(_ => ((_.Index >= start) && (_.Index <= end)) == false)
                    .Select(_ => _.Data)
                    .ToArray();

                x.List.DeleteMany(Builders<ListDto>.Filter.Eq(_ => _.Key, key) & Builders<ListDto>.Filter.In(_ => _.Id, items));
            });
        }

        public override void SetRangeInHash(string key, IEnumerable<KeyValuePair<string, string>> keyValuePairs)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            if (keyValuePairs == null)
                throw new ArgumentNullException("keyValuePairs");

            foreach (var keyValuePair in keyValuePairs)
            {
                var pair = keyValuePair;

                QueueCommand(x => x.Hash.UpdateMany(
                    Builders<HashDto>.Filter.Eq(_ => _.Key, key) & Builders<HashDto>.Filter.Eq(_ => _.Field, pair.Key),
                    Builders<HashDto>.Update.Set(_ => _.Value, pair.Value),
                    new UpdateOptions
                    {
                        IsUpsert = true
                    }));
            }
        }

        public override void RemoveHash(string key)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            QueueCommand(x => x.Hash.DeleteMany(Builders<HashDto>.Filter.Eq(_ => _.Key, key)));
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
            if (key == null) throw new ArgumentNullException("key");
            QueueCommand(x => x.Set.UpdateMany(Builders<SetDto>.Filter.Eq(_ => _.Key, key),
                Builders<SetDto>.Update.Set(_ => _.ExpireAt, _connection.GetServerTimeUtc().Add(expireIn))));
        }

        public override void ExpireList(string key, TimeSpan expireIn)
        {
            if (key == null) throw new ArgumentNullException("key");
            QueueCommand(x => x.List.UpdateMany(Builders<ListDto>.Filter.Eq(_ => _.Key, key),
                Builders<ListDto>.Update.Set(_ => _.ExpireAt, _connection.GetServerTimeUtc().Add(expireIn))));
        }

        public override void ExpireHash(string key, TimeSpan expireIn)
        {
            if (key == null) throw new ArgumentNullException("key");
            QueueCommand(x => x.Hash.UpdateMany(Builders<HashDto>.Filter.Eq(_ => _.Key, key),
                Builders<HashDto>.Update.Set(_ => _.ExpireAt, _connection.GetServerTimeUtc().Add(expireIn))));
        }

        public override void PersistSet(string key)
        {
            if (key == null) throw new ArgumentNullException("key");
            QueueCommand(x => x.Set.UpdateMany(Builders<SetDto>.Filter.Eq(_ => _.Key, key),
                Builders<SetDto>.Update.Set(_ => _.ExpireAt, null)));
        }

        public override void PersistList(string key)
        {
            if (key == null) throw new ArgumentNullException("key");
            QueueCommand(x => x.List.UpdateMany(Builders<ListDto>.Filter.Eq(_ => _.Key, key),
                Builders<ListDto>.Update.Set(_ => _.ExpireAt, null)));
        }

        public override void PersistHash(string key)
        {
            if (key == null) throw new ArgumentNullException("key");
            QueueCommand(x => x.Hash.UpdateMany(Builders<HashDto>.Filter.Eq(_ => _.Key, key),
                Builders<HashDto>.Update.Set(_ => _.ExpireAt, null)));
        }

        public override void AddRangeToSet(string key, IList<string> items)
        {
            if (key == null) throw new ArgumentNullException("key");
            if (items == null) throw new ArgumentNullException("items");

            foreach (var item in items)
            {
                QueueCommand(x => x.Set.UpdateMany(
                    Builders<SetDto>.Filter.Eq(_ => _.Key, key) & Builders<SetDto>.Filter.In(_ => _.Value, items),
                    Builders<SetDto>.Update.Set(_ => _.Score, 0.0),
                    new UpdateOptions
                    {
                        IsUpsert = true
                    }));
            }
        }

        public override void RemoveSet(string key)
        {
            if (key == null) throw new ArgumentNullException("key");
            QueueCommand(x => x.Set.DeleteMany(Builders<SetDto>.Filter.Eq(_ => _.Key, key)));
        }
    }
#pragma warning restore 1591
}