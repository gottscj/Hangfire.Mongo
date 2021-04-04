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
    public class TransactionalMongoWriteOnlyTransaction : MongoWriteOnlyTransaction
    {
        protected IMongoCollection<BsonDocument> JobGraph { get; }

        public TransactionalMongoWriteOnlyTransaction(HangfireDbContext dbContext, MongoStorageOptions storageOptions) 
            : base(dbContext, storageOptions)
        {
            JobGraph = DbContext
                .Database
                .GetCollection<BsonDocument>(DbContext.JobGraph.CollectionNamespace.CollectionName);
        }

        private IClientSessionHandle _session;
        protected virtual IClientSessionHandle SessionHandle
        {
            get
            {
                if (_session == null)
                {
                    _session = DbContext.Client.StartSession();
                    _session.StartTransaction();
                }

                return _session;
            }
        }

        public override void Dispose()
        {
            if (_session?.IsInTransaction == true)
            {
                _session.AbortTransaction();
            }
            _session?.Dispose();
            base.Dispose();
        }

        public override void ExpireJob(string jobId, TimeSpan expireIn)
        {
            var filter = CreateJobIdFilter(jobId);
            var update = new BsonDocument("$set",
                new BsonDocument(nameof(KeyJobDto.ExpireAt), DateTime.UtcNow.Add(expireIn)));
            JobGraph.UpdateOne(SessionHandle, filter, update);
        }

        public override string CreateExpiredJob(Job job, IDictionary<string, string> parameters, DateTime createdAt, TimeSpan expireIn)
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
            
            JobGraph.InsertOne(SessionHandle, jobDto.ToBsonDocument());
            var jobId = jobDto.Id.ToString();

            return jobId;
        }

        public override void PersistJob(string jobId)
        {
            var filter = CreateJobIdFilter(jobId);
            var update = new BsonDocument("$set", new BsonDocument(nameof(KeyJobDto.ExpireAt), BsonNull.Value));
            JobGraph.UpdateOne(SessionHandle, filter, update);
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
            JobGraph.UpdateOne(SessionHandle, filter, update);
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
            JobGraph.UpdateOne(SessionHandle, filter, update);
        }

        public override void SetJobParameter(string id, string name, string value)
        {
            if (id == null)
            {
                throw new ArgumentNullException(nameof(id));
            }

            if (name == null)
            {
                throw new ArgumentNullException(nameof(name));
            }

            var filter = new BsonDocument("_id", ObjectId.Parse(id));
            
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
            JobGraph.UpdateOne(SessionHandle, filter, update);
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
            JobGraph.InsertOne(SessionHandle, jobQueueDto);
            JobsAddedToQueue.Add(queue);
        }
        
        protected override void SetCounter(string key, long amount, TimeSpan? expireIn)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            var filter = new BsonDocument(nameof(CounterDto.Key), key);
            
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
            JobGraph.UpdateOne(SessionHandle, filter, update, new UpdateOptions{IsUpsert = true});
        }
        
        protected override void AddRangeToSet(string key, IList<string> items, double score)
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
                var update = CreateSetUpdate(key, item, score);
                JobGraph.UpdateOne(SessionHandle, filter, update, new UpdateOptions{IsUpsert = true});
            }
        }
        public override void RemoveFromSet(string key, string value)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            var filter = CreateSetFilter(key, value);
            JobGraph.DeleteOne(SessionHandle, filter);
        }

        private readonly List<ListDto> _insertedLists = new List<ListDto>();
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
            JobGraph.InsertOne(SessionHandle, listDto.ToBsonDocument());
            _insertedLists.Add(listDto);
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

            JobGraph.DeleteMany(SessionHandle, filter);
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
            var allIds = DbContext.JobGraph.OfType<ListDto>()
                .Find(new BsonDocument())
                .Project(doc => doc.Id)
                .ToList();
            
            // Add LisDto's scheduled for insertion writemodels collection, add it here.
            allIds
                .AddRange(_insertedLists
                    .Where(l => l.Item == key)
                    .Select(l => l.Id));

            var toTrim = allIds
                .OrderByDescending(id => id.Timestamp)    
                .Select((id, i) => new {Index = i + 1, Id = id})
                .Where(_ => (_.Index >= start && (_.Index <= end)) == false)
                .Select(_ => _.Id)
                .ToList();

            var filter = new BsonDocument
            {
                ["_id"] = new BsonDocument("$in", new BsonArray(toTrim)),
                [nameof(ListDto.Item)] = key
            };
            JobGraph.DeleteMany(SessionHandle, filter);
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
            
            var filter = new BsonDocument(nameof(HashDto.Key), key);

            JobGraph.UpdateOne(SessionHandle, filter, update, new UpdateOptions {IsUpsert = true});
        }
        
        public override void RemoveHash(string key)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            var filter = new BsonDocument(nameof(HashDto.Key), key);
            JobGraph.DeleteOne(SessionHandle, filter);
        }
        
        public override void Commit()
        {
            try
            {
                SessionHandle.CommitTransaction();
            }
            catch (Exception e)
            {
                Logger.ErrorException(e.Message, e);
                SessionHandle.AbortTransaction();
                throw;
            }
            
            
            if (StorageOptions.UseNotificationsCollection)
            {
                SignalJobsAddedToQueues(JobsAddedToQueue);
            }
        }
        public override void ExpireSet(string key, TimeSpan expireIn)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            var filter = CreateSetFilter(key);

            var update = new BsonDocument("$set",
                new BsonDocument(nameof(SetDto.ExpireAt), DateTime.UtcNow.Add(expireIn)));

            JobGraph.UpdateMany(SessionHandle, filter, update);
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
            JobGraph.UpdateMany(SessionHandle, filter, update);
        }
        
        public override void ExpireHash(string key, TimeSpan expireIn)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            var filter = new BsonDocument(nameof(KeyJobDto.Key), key);

            var update = new BsonDocument("$set",
                new BsonDocument(nameof(HashDto.ExpireAt), DateTime.UtcNow.Add(expireIn)));
            JobGraph.UpdateOne(SessionHandle, filter, update);
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

            JobGraph.UpdateMany(filter, update);
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

            JobGraph.UpdateMany(SessionHandle, filter, update);
        }
        
        public override void PersistHash(string key)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            var filter = new BsonDocument(nameof(KeyJobDto.Key), key);

            var update = new BsonDocument("$set",
                new BsonDocument(nameof(HashDto.ExpireAt), BsonNull.Value));

            JobGraph.UpdateOne(SessionHandle, filter, update);
        }
        
        public override void RemoveSet(string key)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            var filter = CreateSetFilter(key);
            JobGraph.DeleteMany(SessionHandle, filter);
        }
    }
#pragma warning restore 1591
}