using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Linq;
using Hangfire.Common;
using Hangfire.Mongo.Database;
using Hangfire.Mongo.Dto;
using Hangfire.Mongo.Helpers;
using Hangfire.Mongo.MongoUtils;
using Hangfire.Mongo.PersistentJobQueue;
using Hangfire.States;
using Hangfire.Storage;
using MongoDB.Bson;
using MongoDB.Driver;

namespace Hangfire.Mongo
{
#pragma warning disable 1591
    public class MongoWriteOnlyTransaction : IWriteOnlyTransaction
    {
        private readonly Queue<Func<HangfireDbContext, Task>> _commandQueue = new Queue<Func<HangfireDbContext, Task>>();

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

        public void Dispose()
        {
        }

        public void ExpireJob(string jobId, TimeSpan expireIn)
        {
            QueueCommand(x => x.Job.UpdateManyAsync(Builders<JobDto>.Filter.Eq(_ => _.Id, int.Parse(jobId)),
                Builders<JobDto>.Update.Set(_ => _.ExpireAt, _connection.GetServerTimeUtc().Add(expireIn))));
        }

        public void PersistJob(string jobId)
        {
            QueueCommand(x => x.Job.UpdateManyAsync(Builders<JobDto>.Filter.Eq(_ => _.Id, int.Parse(jobId)),
                Builders<JobDto>.Update.Set(_ => _.ExpireAt, null)));
        }

        public void SetJobState(string jobId, IState state)
        {
            QueueCommand(x =>
            {
                List<Task> tasks = new List<Task>();

                StateDto stateDto = new StateDto
                {
                    Id = ObjectId.GenerateNewId(),
                    JobId = int.Parse(jobId),
                    Name = state.Name,
                    Reason = state.Reason,
                    CreatedAt = _connection.GetServerTimeUtc(),
                    Data = JobHelper.ToJson(state.SerializeData())
                };
                tasks.Add(x.State.InsertOneAsync(stateDto));

                tasks.Add(x.Job.UpdateManyAsync(
                    Builders<JobDto>.Filter.Eq(_ => _.Id, int.Parse(jobId)),
                    Builders<JobDto>.Update.Set(_ => _.StateId, stateDto.Id)));

                tasks.Add(x.Job.UpdateManyAsync(Builders<JobDto>.Filter.Eq(_ => _.Id, int.Parse(jobId)),
                    Builders<JobDto>.Update.Set(_ => _.StateName, state.Name)));

                return Task.WhenAll(tasks.ToArray());
            });
        }

        public void AddJobState(string jobId, IState state)
        {
            QueueCommand(x => x.State.InsertOneAsync(new StateDto
            {
                Id = ObjectId.GenerateNewId(),
                JobId = int.Parse(jobId),
                Name = state.Name,
                Reason = state.Reason,
                CreatedAt = _connection.GetServerTimeUtc(),
                Data = JobHelper.ToJson(state.SerializeData())
            }));
        }

        public void AddToQueue(string queue, string jobId)
        {
            IPersistentJobQueueProvider provider = _queueProviders.GetProvider(queue);
            IPersistentJobQueue persistentQueue = provider.GetJobQueue(_connection);

            QueueCommand(_ =>
            {
                persistentQueue.Enqueue(queue, jobId);
                return Task.FromResult(0);
            });
        }

        public void IncrementCounter(string key)
        {
            QueueCommand(x => x.Counter.InsertOneAsync(new CounterDto
            {
                Id = ObjectId.GenerateNewId(),
                Key = key,
                Value = +1
            }));
        }

        public void IncrementCounter(string key, TimeSpan expireIn)
        {
            QueueCommand(x => x.Counter.InsertOneAsync(new CounterDto
            {
                Id = ObjectId.GenerateNewId(),
                Key = key,
                Value = +1,
                ExpireAt = _connection.GetServerTimeUtc().Add(expireIn)
            }));
        }

        public void DecrementCounter(string key)
        {
            QueueCommand(x => x.Counter.InsertOneAsync(new CounterDto
            {
                Id = ObjectId.GenerateNewId(),
                Key = key,
                Value = -1
            }));
        }

        public void DecrementCounter(string key, TimeSpan expireIn)
        {
            QueueCommand(x => x.Counter.InsertOneAsync(new CounterDto
            {
                Id = ObjectId.GenerateNewId(),
                Key = key,
                Value = -1,
                ExpireAt = _connection.GetServerTimeUtc().Add(expireIn)
            }));
        }

        public void AddToSet(string key, string value)
        {
            AddToSet(key, value, 0.0);
        }

        public void AddToSet(string key, string value, double score)
        {
            QueueCommand(x => x.Set.UpdateManyAsync(Builders<SetDto>.Filter.Eq(_ => _.Key, key) & Builders<SetDto>.Filter.Eq(_ => _.Value, value),
                Builders<SetDto>.Update.Set(_ => _.Score, score),
                new UpdateOptions
                {
                    IsUpsert = true
                }));
        }

        public void RemoveFromSet(string key, string value)
        {
            QueueCommand(x => x.Set.DeleteManyAsync(
                Builders<SetDto>.Filter.Eq(_ => _.Key, key) &
                Builders<SetDto>.Filter.Eq(_ => _.Value, value)));
        }

        public void InsertToList(string key, string value)
        {
            QueueCommand(x => x.List.InsertOneAsync(new ListDto
            {
                Id = ObjectId.GenerateNewId(),
                Key = key,
                Value = value
            }));
        }

        public void RemoveFromList(string key, string value)
        {
            QueueCommand(x => x.List.DeleteManyAsync(
                Builders<ListDto>.Filter.Eq(_ => _.Key, key) &
                Builders<ListDto>.Filter.Eq(_ => _.Value, value)));
        }

        public void TrimList(string key, int keepStartingFrom, int keepEndingAt)
        {
            QueueCommand(x =>
            {
                int start = keepStartingFrom + 1;
                int end = keepEndingAt + 1;

                ObjectId[] items = ((IEnumerable<ListDto>)AsyncHelper.RunSync(() =>
                        x.List
                        .Find(new BsonDocument())
                        .Project(Builders<ListDto>.Projection.Include(_ => _.Key))
                        .Project(_ => _)
                        .ToListAsync()))
                    .Reverse()
                    .Select((data, i) => new { Index = i + 1, Data = data.Id })
                    .Where(_ => ((_.Index >= start) && (_.Index <= end)) == false)
                    .Select(_ => _.Data)
                    .ToArray();

                return x.List.DeleteManyAsync(Builders<ListDto>.Filter.Eq(_ => _.Key, key) & Builders<ListDto>.Filter.In(_ => _.Id, items));
            });
        }

        public void SetRangeInHash(string key, IEnumerable<KeyValuePair<string, string>> keyValuePairs)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            if (keyValuePairs == null)
                throw new ArgumentNullException("keyValuePairs");

            foreach (var keyValuePair in keyValuePairs)
            {
                var pair = keyValuePair;

                QueueCommand(x => x.Hash.UpdateManyAsync(
                    Builders<HashDto>.Filter.Eq(_ => _.Key, key) & Builders<HashDto>.Filter.Eq(_ => _.Field, pair.Key),
                    Builders<HashDto>.Update.Set(_ => _.Value, pair.Value),
                    new UpdateOptions
                    {
                        IsUpsert = true
                    }));
            }
        }

        public void RemoveHash(string key)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            QueueCommand(x => x.Hash.DeleteManyAsync(Builders<HashDto>.Filter.Eq(_ => _.Key, key)));
        }

        public void Commit()
        {
            List<Task> tasks = new List<Task>();

            foreach (Func<HangfireDbContext, Task> command in _commandQueue)
            {
                tasks.Add(command(_connection));
            }

            Task.WaitAll(tasks.ToArray());
        }

        private void QueueCommand(Func<HangfireDbContext, Task> action)
        {
            _commandQueue.Enqueue(action);
        }
    }
#pragma warning restore 1591
}