using System.Linq;
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

            // Get all state history documents, grouped by JobId
            var allStateHistory = stateHistoryCollection.Find(new BsonDocument()).ToList();
            
            if (!allStateHistory.Any())
            {
                return true;
            }

            // Group by JobId
            var groupedByJobId = allStateHistory
                .GroupBy(doc => doc["JobId"].AsObjectId)
                .ToList();

            foreach (var group in groupedByJobId)
            {
                var jobId = group.Key;

                // Find the job
                var jobFilter = new BsonDocument("_id", jobId);
                var jobDoc = jobGraphCollection.Find(jobFilter).FirstOrDefault();

                if (jobDoc == null)
                {
                    continue;
                }

                // Get states from stateHistory documents, sorted by creation order
                var states = group
                    .Where(h => h.TryGetValue("State", out var stateVal) && stateVal.IsBsonDocument)
                    .Select(h => h["State"].AsBsonDocument)
                    .ToList();

                if (!states.Any())
                {
                    continue;
                }

                // Get existing StateHistory from JobDto
                var existingStateHistory = new BsonArray();
                if (jobDoc.TryGetValue("StateHistory", out var existingHistory) && existingHistory.IsBsonArray)
                {
                    existingStateHistory = existingHistory.AsBsonArray;
                }

                // Append new states to existing ones
                foreach (var state in states)
                {
                    existingStateHistory.Add(state);
                }

                // Set the complete StateHistory array atomically
                var setUpdate = Builders<BsonDocument>.Update.Set("StateHistory", existingStateHistory);
                jobGraphCollection.UpdateOne(jobFilter, setUpdate);
            }

            return true;
        }
    }
}

