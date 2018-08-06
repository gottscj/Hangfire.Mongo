using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Hangfire.Mongo.Database;
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
//        private readonly Queue<Action<HangfireDbContext>> _commandQueue = new Queue<Action<HangfireDbContext>>();

        private readonly HangfireDbContext _connection;

        private readonly PersistentJobQueueProviderCollection _queueProviders;

        private readonly IList<WriteModel<BsonDocument>> _writeModels = new List<WriteModel<BsonDocument>>();

        private readonly IList<Tuple<string, string>> _jobsToEnqueue = new List<Tuple<string, string>>();
        
        public MongoWriteOnlyTransaction(HangfireDbContext connection,
            PersistentJobQueueProviderCollection queueProviders)
        {
            _connection = connection ?? throw new ArgumentNullException(nameof(connection));
            _queueProviders = queueProviders ?? throw new ArgumentNullException(nameof(queueProviders));
        }

        public override void Dispose()
        {
        }

        public override void ExpireJob(string jobId, TimeSpan expireIn)
        {
            var filter = Builders<JobDto>.Filter.Eq(_ => _.Id, ObjectId.Parse(jobId)).ToBsonDocument();
            var update = Builders<JobDto>.Update.Set(_ => _.ExpireAt, DateTime.UtcNow.Add(expireIn)).ToBsonDocument();
            var writeModel = new UpdateOneModel<BsonDocument>(filter, update);
            _writeModels.Add(writeModel);
//            QueueCommand(x => x.Jobs.OfType<JobDto>().UpdateMany(Builders<JobDto>.Filter.Eq(_ => _.Id, ObjectId.Parse(jobId)),
//                Builders<JobDto>.Update.Set(_ => _.ExpireAt, DateTime.UtcNow.Add(expireIn))));
        }

        public override void PersistJob(string jobId)
        {
            var filter = Builders<JobDto>.Filter.Eq(_ => _.Id, ObjectId.Parse(jobId)).ToBsonDocument();
            var update = Builders<JobDto>.Update.Set(_ => _.ExpireAt, null).ToBsonDocument();
            var writeModel = new UpdateOneModel<BsonDocument>(filter, update);
            _writeModels.Add(writeModel);
//            QueueCommand(x => x.Jobs.OfType<JobDto>().UpdateMany(Builders<JobDto>.Filter.Eq(_ => _.Id, ObjectId.Parse(jobId)),
//                ));
        }

        public override void SetJobState(string jobId, IState state)
        {
            var filter = Builders<JobDto>.Filter.Eq(_ => _.Id, ObjectId.Parse(jobId)).ToBsonDocument();
            var update = Builders<JobDto>
                .Update
                .Set(j => j.StateName, state.Name)
                .Push(j => j.StateHistory, new StateDto
                {
                    Name = state.Name,
                    Reason = state.Reason,
                    CreatedAt = DateTime.UtcNow,
                    Data = state.SerializeData()
                }).ToBsonDocument();
            
            var writeModel = new UpdateOneModel<BsonDocument>(filter, update);
            
            _writeModels.Add(writeModel);
//            QueueCommand(x =>
//            {
//                var update = Builders<JobDto>
//                    .Update
//                    .Set(j => j.StateName, state.Name)
//                    .Push(j => j.StateHistory, new StateDto
//                    {
//                        Name = state.Name,
//                        Reason = state.Reason,
//                        CreatedAt = DateTime.UtcNow,
//                        Data = state.SerializeData()
//                    });
//
//                x.Jobs.OfType<JobDto>().UpdateOne(j => j.Id == ObjectId.Parse(jobId), update);
//            });
        }

        public override void AddJobState(string jobId, IState state)
        {
            var filter = Builders<JobDto>.Filter.Eq(_ => _.Id, ObjectId.Parse(jobId)).ToBsonDocument();
            var update = Builders<JobDto>.Update
                .Push(j => j.StateHistory, new StateDto
                {
                    Name = state.Name,
                    Reason = state.Reason,
                    CreatedAt = DateTime.UtcNow,
                    Data = state.SerializeData()
                }).ToBsonDocument();
            
            var writeModel = new UpdateOneModel<BsonDocument>(filter, update);
            
            _writeModels.Add(writeModel);
//            QueueCommand(x =>
//            {
//                var update = Builders<JobDto>.Update
//                    .Push(j => j.StateHistory, new StateDto
//                    {
//                        Name = state.Name,
//                        Reason = state.Reason,
//                        CreatedAt = DateTime.UtcNow,
//                        Data = state.SerializeData()
//                    });
//
//                x.Jobs.OfType<JobDto>().UpdateOne(j => j.Id == ObjectId.Parse(jobId), update);
//            });
        }

        public override void AddToQueue(string queue, string jobId)
        {
            _jobsToEnqueue.Add(Tuple.Create(queue, jobId));
//            IPersistentJobQueueProvider provider = _queueProviders.GetProvider(queue);
//            IPersistentJobQueue persistentQueue = provider.GetJobQueue(_connection);
//
//            
//            QueueCommand(_ =>
//            {
//                persistentQueue.Enqueue(queue, jobId);
//            });
        }

        public override void IncrementCounter(string key)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            var counterDto = new CounterDto
            {
                Id = ObjectId.GenerateNewId(),
                Key = key,
                Value = +1L
            };
            var writeModel = new InsertOneModel<BsonDocument>(counterDto.ToBsonDocument());
            _writeModels.Add(writeModel);
//            QueueCommand(x => x.Jobs.InsertOne(new CounterDto
//            {
//                Id = ObjectId.GenerateNewId(),
//                Key = key,
//                Value = +1L
//            }));
        }

        public override void IncrementCounter(string key, TimeSpan expireIn)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            var counterDto = new CounterDto
            {
                Id = ObjectId.GenerateNewId(),
                Key = key,
                Value = +1L,
                ExpireAt = DateTime.UtcNow.Add(expireIn)
            };
            var writeModel = new InsertOneModel<BsonDocument>(counterDto.ToBsonDocument());
            _writeModels.Add(writeModel);
//            QueueCommand(x => x.Jobs.InsertOne(new CounterDto
//            {
//                Id = ObjectId.GenerateNewId(),
//                Key = key,
//                Value = +1L,
//                ExpireAt = DateTime.UtcNow.Add(expireIn)
//            }));
        }

        public override void DecrementCounter(string key)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            var counterDto = new CounterDto
            {
                Id = ObjectId.GenerateNewId(),
                Key = key,
                Value = -1L
            };
            var writeModel = new InsertOneModel<BsonDocument>(counterDto.ToBsonDocument());
            _writeModels.Add(writeModel);
            
//            QueueCommand(x => x.Jobs.InsertOne(new CounterDto
//            {
//                Id = ObjectId.GenerateNewId(),
//                Key = key,
//                Value = -1L
//            }));
        }

        public override void DecrementCounter(string key, TimeSpan expireIn)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            var counterDto = new CounterDto
            {
                Id = ObjectId.GenerateNewId(),
                Key = key,
                Value = -1L,
                ExpireAt = DateTime.UtcNow.Add(expireIn)
            };
            var writeModel = new InsertOneModel<BsonDocument>(counterDto.ToBsonDocument());
            _writeModels.Add(writeModel);
            
//            QueueCommand(x => x.Jobs.InsertOne(new CounterDto
//            {
//                Id = ObjectId.GenerateNewId(),
//                Key = key,
//                Value = -1L,
//                ExpireAt = DateTime.UtcNow.Add(expireIn)
//            }));
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
            var setTypesOnInsert = builder.SetOnInsert("_t", new[] { nameof(BaseJobDto), nameof(KeyJobDto), nameof(SetDto) });
            var setExpireAt = builder.SetOnInsert(_ => _.ExpireAt, null);
            var update = builder.Combine(set, setTypesOnInsert, setExpireAt);

            var filter = (Builders<SetDto>.Filter.Eq(_ => _.Key, key) & Builders<SetDto>.Filter.Eq(_ => _.Value, value))
                .ToBsonDocument();
            
            var writeModel = new UpdateOneModel<BsonDocument>(filter, update.ToBsonDocument()){IsUpsert = true};
            _writeModels.Add(writeModel);
//            QueueCommand(x => x.Jobs
//                .OfType<SetDto>()
//                .UpdateOne(
//                    Builders<SetDto>.Filter.Eq(_ => _.Key, key) & Builders<SetDto>.Filter.Eq(_ => _.Value, value),
//                    update,
//                    new UpdateOptions
//                    {
//                        IsUpsert = true
//                    }));
        }

        public override void RemoveFromSet(string key, string value)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            var filter = (Builders<SetDto>.Filter.Eq(_ => _.Key, key) &
                          Builders<SetDto>.Filter.Eq(_ => _.Value, value))
                .ToBsonDocument();

            var writeModel = new DeleteManyModel<BsonDocument>(filter);
            _writeModels.Add(writeModel);
//            QueueCommand(x => x.Jobs
//                .OfType<SetDto>()
//                .DeleteMany(
//                    Builders<SetDto>.Filter.Eq(_ => _.Key, key) &
//                    Builders<SetDto>.Filter.Eq(_ => _.Value, value)));
        }

        public override void InsertToList(string key, string value)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            var listDto = new ListDto
            {
                Id = ObjectId.GenerateNewId(),
                Key = key,
                Value = value
            };
            
            var writeModel = new InsertOneModel<BsonDocument>(listDto.ToBsonDocument());
            _writeModels.Add(writeModel);
            
//            QueueCommand(x => x.Jobs.InsertOne(new ListDto
//            {
//                Id = ObjectId.GenerateNewId(),
//                Key = key,
//                Value = value
//            }));
        }

        public override void RemoveFromList(string key, string value)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            var filter = (Builders<ListDto>.Filter.Eq(_ => _.Key, key) &
                          Builders<ListDto>.Filter.Eq(_ => _.Value, value))
                .ToBsonDocument();

            var writeModel = new DeleteManyModel<BsonDocument>(filter);
            _writeModels.Add(writeModel);
            
//            QueueCommand(x => x.Jobs
//                .OfType<ListDto>()
//                .DeleteMany(
//                    Builders<ListDto>.Filter.Eq(_ => _.Key, key) &
//                    Builders<ListDto>.Filter.Eq(_ => _.Value, value)));
        }

        public override void TrimList(string key, int keepStartingFrom, int keepEndingAt)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }
            
            var start = keepStartingFrom + 1;
            var end = keepEndingAt + 1;

            var listIds = ((IEnumerable<ListDto>)_connection.JobGraph.OfType<ListDto>()
                    .Find(new BsonDocument())
                    .Project(Builders<ListDto>.Projection.Include(_ => _.Key))
                    .Project(_ => _)
                    .ToList())
                .Reverse()
                .Select((data, i) => new { Index = i + 1, Data = data.Id })
                .Where(_ => ((_.Index >= start) && (_.Index <= end)) == false)
                .Select(_ => _.Data)
                .ToArray();
            var filter = (Builders<ListDto>.Filter.Eq(_ => _.Key, key) &
                          Builders<ListDto>.Filter.In(_ => _.Id, listIds))
                .ToBsonDocument();
            var writeModel = new DeleteManyModel<BsonDocument>(filter);
            _writeModels.Add(writeModel);
//            QueueCommand(x =>
//            {
//                int start = keepStartingFrom + 1;
//                int end = keepEndingAt + 1;
//
//                ObjectId[] items = ((IEnumerable<ListDto>)x.JobGraph.OfType<ListDto>()
//                        .Find(new BsonDocument())
//                        .Project(Builders<ListDto>.Projection.Include(_ => _.Key))
//                        .Project(_ => _)
//                        .ToList())
//                    .Reverse()
//                    .Select((data, i) => new { Index = i + 1, Data = data.Id })
//                    .Where(_ => ((_.Index >= start) && (_.Index <= end)) == false)
//                    .Select(_ => _.Data)
//                    .ToArray();
//
//                x.JobGraph
//                    .OfType<ListDto>()
//                    .DeleteMany(Builders<ListDto>.Filter.Eq(_ => _.Key, key) &
//                                Builders<ListDto>.Filter.In(_ => _.Id, items));
//            });
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
            var setTypesOnInsert = builder.SetOnInsert("_t", new[] { nameof(BaseJobDto), nameof(KeyJobDto), nameof(HashDto) });
            var setExpireAt = builder.SetOnInsert(_ => _.ExpireAt, null);

            foreach (var keyValuePair in keyValuePairs)
            {
                var field = keyValuePair.Key;
                var value = keyValuePair.Value;

                var set = builder.Set(_ => _.Value, value);
                var update = builder.Combine(set, setTypesOnInsert, setExpireAt);
                var filter = (Builders<HashDto>.Filter.Eq(_ => _.Key, key) &
                              Builders<HashDto>.Filter.Eq(_ => _.Field, field))
                    .ToBsonDocument();
                
                var writeModel = new UpdateManyModel<BsonDocument>(filter, update.ToBsonDocument());
                _writeModels.Add(writeModel);
//                QueueCommand(x =>
//                {
//                    var set = builder.Set(_ => _.Value, value);
//                    var update = builder.Combine(set, setTypesOnInsert, setExpireAt);
//                    x.JobGraph.OfType<HashDto>()
//                        .UpdateMany(
//                            Builders<HashDto>.Filter.Eq(_ => _.Key, key) &
//                            Builders<HashDto>.Filter.Eq(_ => _.Field, field),
//                            update,
//                            new UpdateOptions
//                            {
//                                IsUpsert = true
//                            });
//                });
            }
        }

        public override void RemoveHash(string key)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            var filter = Builders<HashDto>.Filter.Eq(_ => _.Key, key).ToBsonDocument();
            var writeModel = new DeleteManyModel<BsonDocument>(filter);
            _writeModels.Add(writeModel);
//            QueueCommand(x => x.JobGraph.OfType<HashDto>().DeleteMany(Builders<HashDto>.Filter.Eq(_ => _.Key, key)));
        }

        public override void Commit()
        {
            var writeTask = _connection
                .Database
                .GetCollection<BsonDocument>(_connection.JobGraph.CollectionNamespace.CollectionName)
                .BulkWriteAsync(_writeModels);

            var enqueueTask = Task.Run(() =>
            {
                foreach (var tuple in _jobsToEnqueue)
                {
                    var queue = tuple.Item1;
                    var jobId = tuple.Item2;

                    IPersistentJobQueueProvider provider = _queueProviders.GetProvider(queue);
                    IPersistentJobQueue persistentQueue = provider.GetJobQueue(_connection);
                    persistentQueue.Enqueue(queue, jobId);
                }
            });
            // make sure to run tasks on default task scheduler
            Task.Run(() => Task.WhenAll(writeTask, enqueueTask)).GetAwaiter().GetResult();
            
            // TODO: Using this lock leads to deadlocks.
            //       Investigate why and reintroduce when ready.
            //using (new MongoDistributedLock("WriteOnlyTransaction", TimeSpan.FromSeconds(30), _connection, _options))
//            {
//                foreach (var action in _commandQueue)
//                {
//                    action.Invoke(_connection);
//                }
//            }
        }

//        private void QueueCommand(Action<HangfireDbContext> action)
//        {
//            _commandQueue.Enqueue(action);
//        }


        // New methods to support Hangfire pro feature - batches.


        public override void ExpireSet(string key, TimeSpan expireIn)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            QueueCommand(x => x
                .JobGraph
                .OfType<SetDto>()
                .UpdateMany(Builders<SetDto>.Filter.Eq(_ => _.Key, key),
                Builders<SetDto>.Update.Set(_ => _.ExpireAt, DateTime.UtcNow.Add(expireIn))));
        }

        public override void ExpireList(string key, TimeSpan expireIn)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            QueueCommand(x => x.JobGraph
                .OfType<ListDto>()
                .UpdateMany(Builders<ListDto>.Filter.Eq(_ => _.Key, key),
                Builders<ListDto>.Update.Set(_ => _.ExpireAt, DateTime.UtcNow.Add(expireIn))));
        }

        public override void ExpireHash(string key, TimeSpan expireIn)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            QueueCommand(x => x.JobGraph
                .OfType<HashDto>()
                .UpdateMany(Builders<HashDto>.Filter.Eq(_ => _.Key, key),
                Builders<HashDto>.Update.Set(_ => _.ExpireAt, DateTime.UtcNow.Add(expireIn))));
        }

        public override void PersistSet(string key)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            QueueCommand(x => x.JobGraph
                .OfType<SetDto>()
                .UpdateMany(Builders<SetDto>.Filter.Eq(_ => _.Key, key),
                    Builders<SetDto>.Update.Set(_ => _.ExpireAt, null)));
        }

        public override void PersistList(string key)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            QueueCommand(x => x.JobGraph
                .OfType<ListDto>()
                .UpdateMany(Builders<ListDto>.Filter.Eq(_ => _.Key, key),
                    Builders<ListDto>.Update.Set(_ => _.ExpireAt, null)));
        }

        public override void PersistHash(string key)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            QueueCommand(x => x.JobGraph
                .OfType<HashDto>()
                .UpdateMany(Builders<HashDto>.Filter.Eq(_ => _.Key, key),
                    Builders<HashDto>.Update.Set(_ => _.ExpireAt, null)));
        }

        public override void AddRangeToSet(string key, IList<string> items)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }
            if (items == null)
            {
                throw new ArgumentNullException(nameof(items));
            }
            var builder = Builders<SetDto>.Update;


            var setTypesOnInsert = builder.SetOnInsert("_t", new[] {  nameof(BaseJobDto), nameof(KeyJobDto), nameof(SetDto) });
            var setExpireAt = builder.SetOnInsert(_ => _.ExpireAt, null);
            var set = builder.Set(_ => _.Score, 0.0);
            var update = builder.Combine(set, setTypesOnInsert, setExpireAt);

            foreach (var item in items)
            {
                QueueCommand(x =>
                {
                    x.JobGraph
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
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }
            QueueCommand(x => x.JobGraph
                .OfType<SetDto>()
                .DeleteMany(Builders<SetDto>.Filter.Eq(_ => _.Key, key)));
        }
    }

#pragma warning restore 1591
}