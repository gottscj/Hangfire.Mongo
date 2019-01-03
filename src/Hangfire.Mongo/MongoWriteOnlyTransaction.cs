using System;
using System.Collections.Generic;
using System.Linq;
using Hangfire.Common;
using Hangfire.Logging;
using Hangfire.Mongo.Database;
using Hangfire.Mongo.Dto;
using Hangfire.States;
using Hangfire.Storage;
using MongoDB.Bson;
using MongoDB.Driver;

namespace Hangfire.Mongo
{
#pragma warning disable 1591

    public sealed class MongoWriteOnlyTransaction : JobStorageTransaction
    {
        private static readonly ILog Logger = LogProvider.For<MongoWriteOnlyTransaction>();
        
        private readonly HangfireDbContext _dbContext;

        private readonly IList<WriteModel<BsonDocument>> _writeModels = new List<WriteModel<BsonDocument>>();

        public MongoWriteOnlyTransaction(HangfireDbContext dbContext)
        {
            _dbContext = dbContext ?? throw new ArgumentNullException(nameof(dbContext));
        }

        public override void Dispose()
        {
        }

        public override void ExpireJob(string jobId, TimeSpan expireIn)
        {
            var filter = CreateJobIdFilter(jobId);
            var update = new BsonDocument("$set",
                new BsonDocument(nameof(KeyJobDto.ExpireAt), DateTime.UtcNow.Add(expireIn)));

            var writeModel = new UpdateOneModel<BsonDocument>(filter, update);
            _writeModels.Add(writeModel);
        }
        
        public string CreateExpiredJob(Job job, IDictionary<string, string> parameters, DateTime createdAt,
            TimeSpan expireIn)
        {
            if (job == null)
                throw new ArgumentNullException(nameof(job));

            if (parameters == null)
                throw new ArgumentNullException(nameof(parameters));

            var invocationData = InvocationData.Serialize(job);

            var jobDto = new JobDto
            {
                Id = ObjectId.GenerateNewId(),
                InvocationData = JobHelper.ToJson(invocationData),
                Arguments = invocationData.Arguments,
                Parameters = parameters.ToDictionary(kv => kv.Key, kv => kv.Value),
                CreatedAt = createdAt,
                ExpireAt = createdAt.Add(expireIn)
            };

            var writeModel = new InsertOneModel<BsonDocument>(jobDto.ToBsonDocument());
            _writeModels.Add(writeModel);

            var jobId = jobDto.Id.ToString();

            return jobId;
        }

        public override void PersistJob(string jobId)
        {
            var filter = CreateJobIdFilter(jobId);
            var update = new BsonDocument("$set", new BsonDocument(nameof(KeyJobDto.ExpireAt), BsonNull.Value));

            var writeModel = new UpdateOneModel<BsonDocument>(filter, update);
            _writeModels.Add(writeModel);
        }

        public override void SetJobState(string jobId, IState state)
        {
            var filter = CreateJobIdFilter(jobId);
            var stateDto = new StateDto
            {
                Name = state.Name,
                Reason = state.Reason,
                CreatedAt = DateTime.UtcNow,
                Data = state.SerializeData()
            }.ToBsonDocument();

            var update = new BsonDocument
            {
                ["$set"] = new BsonDocument(nameof(JobDto.StateName), state.Name),
                ["$push"] = new BsonDocument(nameof(JobDto.StateHistory), stateDto)
            };
            var writeModel = new UpdateOneModel<BsonDocument>(filter, update);

            _writeModels.Add(writeModel);
        }

        public override void AddJobState(string jobId, IState state)
        {
            var filter = CreateJobIdFilter(jobId);
            var stateDto = new StateDto
            {
                Name = state.Name,
                Reason = state.Reason,
                CreatedAt = DateTime.UtcNow,
                Data = state.SerializeData()
            }.ToBsonDocument();

            var update = new BsonDocument("$push", new BsonDocument(nameof(JobDto.StateHistory), stateDto));

            var writeModel = new UpdateOneModel<BsonDocument>(filter, update);

            _writeModels.Add(writeModel);
        }
        
        public void SetJobParameter(string id, string name, string value)
        {
            if (id == null)
            {
                throw new ArgumentNullException(nameof(id));
            }

            if (name == null)
            {
                throw new ArgumentNullException(nameof(name));
            }

            var filter = new BsonDocument("$and", new BsonArray
            {
                new BsonDocument("_id", ObjectId.Parse(id)),
                new BsonDocument("_t", nameof(JobDto))
            });
            
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
            
            var writeModel = new UpdateOneModel<BsonDocument>(filter, update);
            
            _writeModels.Add(writeModel);
        }

        public override void AddToQueue(string queue, string jobId)
        {
            var jobQueueDto = new JobQueueDto
            {
                JobId = ObjectId.Parse(jobId),
                Queue = queue,
                Id = ObjectId.GenerateNewId(),
                FetchedAt = null
            }.ToBsonDocument();
            
            var writeModel = new InsertOneModel<BsonDocument>(jobQueueDto);
            _writeModels.Add(writeModel);
        }

        public override void IncrementCounter(string key)
        {
            SetCounter(key, 1, null);
        }

        public override void IncrementCounter(string key, TimeSpan expireIn)
        {
            SetCounter(key, 1, expireIn);
        }
        
       public override void DecrementCounter(string key)
        {
            SetCounter(key, -1, null);
        }

        public override void DecrementCounter(string key, TimeSpan expireIn)
        {
           SetCounter(key, -1, expireIn);
        }
        
        private void SetCounter(string key, long amount, TimeSpan? expireIn)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            var filter = new BsonDocument("$and", new BsonArray
            {
                new BsonDocument(nameof(CounterDto.Key), key),
                new BsonDocument("_t", nameof(CounterDto)),
            });
            
            BsonValue bsonDate = BsonNull.Value;
            if (expireIn != null)
            {
                bsonDate = BsonValue.Create(DateTime.UtcNow.Add(expireIn.Value));
            }
            
            var update = new BsonDocument
            {
                ["$inc"] = new BsonDocument(nameof(CounterDto.Value), amount),
                ["$set"] = new BsonDocument(nameof(KeyJobDto.ExpireAt), bsonDate),
                ["$setOnInsert"] = new BsonDocument
                {
                    ["_t"] = new BsonArray {nameof(BaseJobDto), nameof(ExpiringJobDto), nameof(KeyJobDto), nameof(CounterDto)},
                }
            };
            
            var writeModel = new UpdateOneModel<BsonDocument>(filter, update){IsUpsert = true};
            _writeModels.Add(writeModel);
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

            var filter = CreateSetFilter(key, value);
            var update = CreateSetUpdate(value, score);

            var writeModel = new UpdateOneModel<BsonDocument>(filter, update) {IsUpsert = true};
            _writeModels.Add(writeModel);
        }

        public override void RemoveFromSet(string key, string value)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            var filter = CreateSetFilter(key, value);

            var writeModel = new DeleteOneModel<BsonDocument>(filter);
            _writeModels.Add(writeModel);
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
                Item = key,
                Value = value
            };

            var writeModel = new InsertOneModel<BsonDocument>(listDto.ToBsonDocument());
            _writeModels.Add(writeModel);
        }

        public override void RemoveFromList(string key, string value)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            var filter = new BsonDocument("$and", new BsonArray
            {
                new BsonDocument(nameof(ListDto.Item), key),
                new BsonDocument(nameof(ListDto.Value), value),
                new BsonDocument("_t", nameof(ListDto))
            });

            var writeModel = new DeleteManyModel<BsonDocument>(filter);
            _writeModels.Add(writeModel);
        }

        public override void TrimList(string key, int keepStartingFrom, int keepEndingAt)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            var start = keepStartingFrom + 1;
            var end = keepEndingAt + 1;

            // get all ids
            var allIds = _dbContext.JobGraph.OfType<ListDto>()
                .Find(new BsonDocument())
                .Project(doc => doc.Id)
                .ToList();
            
            // Add LisDto's scheduled for insertion writemodels collection, add it here.
            allIds
                .AddRange(_writeModels.OfType<InsertOneModel<BsonDocument>>()
                    .Where(model => ListDtoHasItem(key, model))
                    .Select(model => model.Document["_id"].AsObjectId));

            var toTrim = allIds
                .OrderByDescending(id => id.Timestamp)    
                .Select((id, i) => new {Index = i + 1, Id = id})
                .Where(_ => (_.Index >= start && (_.Index <= end)) == false)
                .Select(_ => _.Id)
                .ToList();

            var filter = new BsonDocument("$and", new BsonArray
            {
                new BsonDocument(nameof(ListDto.Item), key),
                new BsonDocument("_id", new BsonDocument("$in", new BsonArray(toTrim))),
                new BsonDocument("_t", nameof(ListDto))
            });

            var writeModel = new DeleteManyModel<BsonDocument>(filter);
            _writeModels.Add(writeModel);
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

           var fields = new BsonDocument();
            
            foreach (var pair in keyValuePairs)
            {
                var field = pair.Key;
                var value = pair.Value;
                fields[$"{nameof(HashDto.Fields)}.{field}"] = value;
            }
            
            var update = new BsonDocument
            {
                ["$set"] = fields,
                ["$setOnInsert"] = new BsonDocument
                {
                    ["_t"] = new BsonArray {nameof(BaseJobDto), nameof(ExpiringJobDto), nameof(KeyJobDto), nameof(HashDto)},
                    [nameof(HashDto.ExpireAt)] = BsonNull.Value
                }
            };
            
            var filter = new BsonDocument("$and", new BsonArray
            {
                new BsonDocument(nameof(HashDto.Key), key),
                new BsonDocument("_t", nameof(HashDto))
            });

            var writeModel = new UpdateOneModel<BsonDocument>(filter, update){IsUpsert = true};
            _writeModels.Add(writeModel);
        }

        public override void RemoveHash(string key)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            var filter = new BsonDocument(nameof(HashDto.Key), key);
            var writeModel = new DeleteManyModel<BsonDocument>(filter);
            _writeModels.Add(writeModel);
        }

        public override void Commit()
        {
            if (Logger.IsDebugEnabled())
            {
                Logger.Debug($"\r\nCommit:\r\n {string.Join("\r\n", _writeModels.Select(SerializeWriteModel))}");
            }

            if (_writeModels.Any())
            {
                _dbContext
                    .Database
                    .GetCollection<BsonDocument>(_dbContext.JobGraph.CollectionNamespace.CollectionName)
                    .BulkWrite(_writeModels);
            }
        }

        private string SerializeWriteModel(WriteModel<BsonDocument> writeModel)
        {
            string serializedDoc;

            var serializer = _dbContext
                .Database
                .GetCollection<BsonDocument>(_dbContext.JobGraph.CollectionNamespace.CollectionName)
                .DocumentSerializer;

            var registry = _dbContext.JobGraph.Settings.SerializerRegistry;

            switch (writeModel.ModelType)
            {
                case WriteModelType.InsertOne:
                    serializedDoc = ((InsertOneModel<BsonDocument>) writeModel).Document.ToJson();
                    break;
                case WriteModelType.DeleteOne:
                    serializedDoc = ((DeleteOneModel<BsonDocument>) writeModel).Filter.Render(serializer, registry)
                        .ToJson();
                    break;
                case WriteModelType.DeleteMany:
                    serializedDoc = ((DeleteManyModel<BsonDocument>) writeModel).Filter.Render(serializer, registry)
                        .ToJson();
                    break;
                case WriteModelType.ReplaceOne:

                    serializedDoc = new Dictionary<string, BsonDocument>
                    {
                        ["Filter"] = ((ReplaceOneModel<BsonDocument>) writeModel).Filter.Render(serializer, registry),
                        ["Replacement"] = ((ReplaceOneModel<BsonDocument>) writeModel).Replacement
                    }.ToJson();
                    break;
                case WriteModelType.UpdateOne:
                    serializedDoc = new Dictionary<string, BsonDocument>
                    {
                        ["Filter"] = ((UpdateOneModel<BsonDocument>) writeModel).Filter.Render(serializer, registry),
                        ["Update"] = ((UpdateOneModel<BsonDocument>) writeModel).Update.Render(serializer, registry)
                    }.ToJson();
                    break;
                case WriteModelType.UpdateMany:
                    serializedDoc = new Dictionary<string, BsonDocument>
                    {
                        ["Filter"] = ((UpdateManyModel<BsonDocument>) writeModel).Filter.Render(serializer, registry),
                        ["Update"] = ((UpdateManyModel<BsonDocument>) writeModel).Update.Render(serializer, registry)
                    }.ToJson();
                    break;
                default:
                    throw new ArgumentOutOfRangeException();
            }
            
            return $"{writeModel.ModelType}: {serializedDoc}";
        }
        // New methods to support Hangfire pro feature - batches.


        public override void ExpireSet(string key, TimeSpan expireIn)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            var filter = CreateSetFilter(key);

            var update = new BsonDocument("$set",
                new BsonDocument(nameof(SetDto.ExpireAt), DateTime.UtcNow.Add(expireIn)));

            var writeModel = new UpdateManyModel<BsonDocument>(filter, update);
            _writeModels.Add(writeModel);
        }

        public override void ExpireList(string key, TimeSpan expireIn)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            var filter = new BsonDocument("$and", new BsonArray
            {
                new BsonDocument(nameof(ListDto.Item), key),
                new BsonDocument("_t", nameof(ListDto))
            });

            var update = new BsonDocument("$set",
                new BsonDocument(nameof(ListDto.ExpireAt), DateTime.UtcNow.Add(expireIn)));
            var writeModel = new UpdateManyModel<BsonDocument>(filter, update);
            _writeModels.Add(writeModel);
        }

        public override void ExpireHash(string key, TimeSpan expireIn)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            var filter = new BsonDocument("$and", new BsonArray
            {
                new BsonDocument(nameof(KeyJobDto.Key), key),
                new BsonDocument("_t", nameof(HashDto))
            });

            var update = new BsonDocument("$set",
                new BsonDocument(nameof(HashDto.ExpireAt), DateTime.UtcNow.Add(expireIn)));
            var writeModel = new UpdateOneModel<BsonDocument>(filter, update);
            _writeModels.Add(writeModel);
        }


        public override void PersistSet(string key)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            var filter = CreateSetFilter(key);

            var update = new BsonDocument("$set",
                new BsonDocument(nameof(SetDto.ExpireAt), BsonNull.Value));

            var writeModel = new UpdateManyModel<BsonDocument>(filter, update);
            _writeModels.Add(writeModel);
        }

        public override void PersistList(string key)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            var filter = new BsonDocument("$and", new BsonArray
            {
                new BsonDocument(nameof(ListDto.Item), key),
                new BsonDocument("_t", nameof(ListDto))
            });

            var update = new BsonDocument("$set",
                new BsonDocument(nameof(ListDto.ExpireAt), BsonNull.Value));

            var writeModel = new UpdateManyModel<BsonDocument>(filter, update);
            _writeModels.Add(writeModel);
        }

        public override void PersistHash(string key)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            var filter = new BsonDocument("$and", new BsonArray
            {
                new BsonDocument(nameof(KeyJobDto.Key), key),
                new BsonDocument("_t", nameof(HashDto))
            });

            var update = new BsonDocument("$set",
                new BsonDocument(nameof(HashDto.ExpireAt), BsonNull.Value));

            var writeModel = new UpdateOneModel<BsonDocument>(filter, update);
            _writeModels.Add(writeModel);
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

            foreach (var item in items)
            {
                var filter = CreateSetFilter(key, item);
                var update = CreateSetUpdate(item, 0.0);
                
                var writeModel = new UpdateOneModel<BsonDocument>(filter, update) {IsUpsert = true};
                _writeModels.Add(writeModel);
            }
        }

        public override void RemoveSet(string key)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            var filter = CreateSetFilter(key);
            var writeModel = new DeleteManyModel<BsonDocument>(filter);
            _writeModels.Add(writeModel);
        }

        private static BsonDocument CreateJobIdFilter(string jobId)
        {
            return new BsonDocument("$and", new BsonArray
            {
                new BsonDocument("_id", ObjectId.Parse(jobId)),
                new BsonDocument("_t", nameof(JobDto))
            });
        }

        private static bool ListDtoHasItem(string key, InsertOneModel<BsonDocument> model)
        {
            return model.Document["_t"].AsBsonArray.Last().AsString == nameof(ListDto) &&
                   model.Document[nameof(ListDto.Item)].AsString == key;
        }

        private static BsonDocument CreateSetFilter(string key, string value)
        {
            var filter = new BsonDocument("$and", new BsonArray
            {
                new BsonDocument(nameof(SetDto.Key), $"{key}<{value}>"),
                new BsonDocument("_t", nameof(SetDto)),
            });
            return filter;
        }
        
        private static BsonDocument CreateSetFilter(string key)
        {
            var filter = new BsonDocument("$and", new BsonArray
            {
                new BsonDocument(nameof(KeyJobDto.Key), new BsonDocument("$regex", $"^{key}")),
                new BsonDocument("_t", nameof(SetDto))
            });
            return filter;
        }
        
        private static BsonDocument CreateSetUpdate(string value, double score)
        {
            var update = new BsonDocument
            {
                ["$set"] = new BsonDocument(nameof(SetDto.Score), score),
                ["$setOnInsert"] = new BsonDocument
                {
                    ["_t"] = new BsonArray {nameof(BaseJobDto), nameof(ExpiringJobDto), nameof(KeyJobDto), nameof(SetDto)},
                    [nameof(SetDto.Value)] = value,
                    [nameof(SetDto.ExpireAt)] = BsonNull.Value
                }
            };
            return update;
        }
    }

#pragma warning restore 1591
}