using System.Collections.Generic;
using System.Linq;
using Hangfire.Mongo.Dto;
using MongoDB.Bson;
using MongoDB.Driver;

namespace Hangfire.Mongo.Migration.Steps.Version20
{
    internal class RemoveJobQueueDto : IMongoMigrationStep
    {
        public MongoSchema TargetSchema => MongoSchema.Version20;
        public long Sequence => 0;
        public bool Execute(IMongoDatabase database, MongoStorageOptions storageOptions, IMongoMigrationContext migrationContext)
        {
            var jobGraph = database.GetCollection<BsonDocument>($@"{storageOptions.Prefix}.jobGraph");

            var jobUpdate = new BsonDocument
            {
                ["$set"] = new BsonDocument
                {
                    ["Queue"] = BsonNull.Value,
                    ["FetchedAt"] = BsonNull.Value
                }
            };

            jobGraph.UpdateMany(new BsonDocument
            {
                ["_t"] = "JobDto"
            }, jobUpdate);

            var jobQueueDtoCursor = jobGraph
                .Find(new BsonDocument("_t", "JobQueueDto"))
                .ToCursor();
            while (jobQueueDtoCursor.MoveNext())
            {
                var writeModels = new List<WriteModel<BsonDocument>>();
                var idsToDelete = new BsonArray();
                foreach (var jobQueue in jobQueueDtoCursor.Current)
                {
                    writeModels.Add(new UpdateOneModel<BsonDocument>(
                                            new BsonDocument { ["_id"] = jobQueue["JobId"] },
                                            new BsonDocument
                                            {
                                                ["$set"] = new BsonDocument
                                                {
                                                    ["Queue"] = jobQueue["Queue"],
                                                    ["FetchedAt"] = jobQueue.TryGetValue("FetchedAt", out var fetchedAt) ? fetchedAt : BsonNull.Value
                                                }
                                            }
                                        ));
                    idsToDelete.Add(jobQueue["_id"]);
                }

                if (writeModels.Any())
                {
                    jobGraph.BulkWrite(writeModels);
                    if (idsToDelete.Any())
                    {
                        jobGraph.DeleteMany(new BsonDocument
                        {
                            ["_id"] = new BsonDocument
                            {
                                ["$in"] = idsToDelete
                            }
                        });
                    }

                }
            }

            var cursor = jobGraph
                .Find(new BsonDocument("_t", "JobDto")
                {
                    ["FetchedAt"] = BsonNull.Value,
                    ["StateName"] = new BsonDocument("$ne", BsonNull.Value)
                })
                .Project(new BsonDocument("StateHistory", 1))
                .ToCursor();

            
            while (cursor.MoveNext())
            {
                var writeModels = new List<WriteModel<BsonDocument>>();
                foreach (var doc in cursor.Current)
                {
                    var stateHistory = doc["StateHistory"].AsBsonArray;
                    var state = stateHistory
                    .Select(h => h.AsBsonDocument)
                    .FirstOrDefault();

                    if (state != null)
                    {
                        var filter = new BsonDocument("_id", doc["_id"]);
                        var update = new BsonDocument
                        {
                            ["$set"] = new BsonDocument
                            {
                                ["FetchedAt"] = state["CreatedAt"]
                            }
                        };
                        writeModels.Add(new UpdateOneModel<BsonDocument>(filter, update));
                    }
                }
                if (writeModels.Any())
                {
                    jobGraph.BulkWrite(writeModels);
                }
            }

            return true;
        }
    }
}