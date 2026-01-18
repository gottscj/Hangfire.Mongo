using System.Collections.Generic;
using System.Linq;
using MongoDB.Bson;
using MongoDB.Driver;

namespace Hangfire.Mongo.Migration.Steps.Version23
{
    /// <summary>
    /// Migrates StateHistory from JobDto documents into the separate stateHistory collection.
    /// </summary>
    internal class MigrateStateHistoryToCollectionStep : IMongoMigrationStep
    {
        public MongoSchema TargetSchema => MongoSchema.Version23;
        public long Sequence => 1; // Runs after collection creation (Sequence 0)

        public bool Execute(IMongoDatabase database, MongoStorageOptions storageOptions, IMongoMigrationContext migrationContext)
        {
            var jobGraphCollection = database.GetCollection<BsonDocument>(storageOptions.Prefix + ".jobGraph");
            var stateHistoryCollection = database.GetCollection<BsonDocument>(storageOptions.Prefix + ".stateHistory");

            // Filter for JobDto documents that have StateHistory
            var filter = Builders<BsonDocument>.Filter.And(
                Builders<BsonDocument>.Filter.Eq("_t", new BsonArray { "BaseJobDto", "ExpiringJobDto", "JobDto" }),
                Builders<BsonDocument>.Filter.Exists("StateHistory"),
                Builders<BsonDocument>.Filter.Ne("StateHistory", new BsonArray())
            );

            var jobs = jobGraphCollection.Find(filter).ToList();

            if (!jobs.Any())
            {
                return true;
            }

            foreach (var job in jobs)
            {
                var historyDocuments = new List<BsonDocument>();
                var jobId = job["_id"].AsObjectId;

                if (!job.TryGetValue("StateHistory", out var stateHistoryValue) || 
                    stateHistoryValue.IsBsonNull || 
                    !stateHistoryValue.IsBsonArray)
                {
                    continue;
                }

                var stateHistory = stateHistoryValue.AsBsonArray;

                foreach (var stateDoc in stateHistory.OfType<BsonDocument>())
                {
                    var historyDocument = new BsonDocument
                    {
                        ["_id"] = ObjectId.GenerateNewId(),
                        ["JobId"] = jobId,
                        ["State"] = stateDoc
                    };

                    historyDocuments.Add(historyDocument);
                }
                if (historyDocuments.Any())
                {
                    stateHistoryCollection.InsertMany(historyDocuments);
                }
            }

            return true;
        }
    }
}