using System.Collections.Generic;
using MongoDB.Bson;
using MongoDB.Driver;

namespace Hangfire.Mongo.Migration.Steps.Version24
{
    /// <summary>
    /// Migrates StateHistory documents from the separate stateHistory collection back into the JobDto documents.
    /// This step appends state history entries to the StateHistory field in JobDto documents.
    /// Existing StateHistory entries in JobDto are preserved.
    /// </summary>
    internal class MigrateStateHistoryBackToJobDtoStep : IMongoMigrationStep
    {
        public MongoSchema TargetSchema => MongoSchema.Version24;
        public long Sequence => 1; // Runs after Step 00

        public bool Execute(IMongoDatabase database, MongoStorageOptions storageOptions, IMongoMigrationContext migrationContext)
        {
            var jobGraphCollection = database.GetCollection<BsonDocument>(storageOptions.Prefix + ".jobGraph");
            var stateHistoryCollection = database.GetCollection<BsonDocument>(storageOptions.Prefix + ".stateHistory");

            var jobDtoFilter = Builders<BsonDocument>.Filter.Eq(
                "_t", new BsonArray { "BaseJobDto", "ExpiringJobDto", "JobDto" });
            using var jobs = jobGraphCollection
                .Find(jobDtoFilter, new FindOptions { BatchSize = 100 })
                .Project(Builders<BsonDocument>.Projection.Include("_id").Include("StateHistory"))
                .Sort(Builders<BsonDocument>.Sort.Ascending("_id"))
                .ToCursor();
            while (jobs.MoveNext())
            {
                var updates = new List<WriteModel<BsonDocument>>();
                foreach (var job in jobs.Current)
                {
                    var jobFilter = new BsonDocument("_id", job["_id"]);
                    var stateHistory = new BsonArray();
                    if (job.TryGetValue("StateHistory", out var existingHistory) && existingHistory.IsBsonArray)
                    {
                        stateHistory = existingHistory.AsBsonArray;
                    }

                    // Version23 creates IX_JobId for these per-job history lookups.
                    using var history = stateHistoryCollection
                        .Find(new BsonDocument("JobId", job["_id"]), new FindOptions { BatchSize = 100 })
                        .ToCursor();
                    var hasNewStates = false;
                    while (history.MoveNext())
                    {
                        foreach (var document in history.Current)
                        {
                            if (!document.TryGetValue("State", out var state) || !state.IsBsonDocument)
                            {
                                continue;
                            }
                            stateHistory.Add(state.AsBsonDocument);
                            hasNewStates = true;
                        }
                    }

                    if (!hasNewStates)
                    {
                        continue;
                    }
                    var update = Builders<BsonDocument>.Update.Set("StateHistory", stateHistory);
                    updates.Add(new UpdateOneModel<BsonDocument>(jobFilter, update));
                }

                if (updates.Count > 0)
                {
                    jobGraphCollection.BulkWrite(updates);
                }
            }

            return true;
        }
    }
}
