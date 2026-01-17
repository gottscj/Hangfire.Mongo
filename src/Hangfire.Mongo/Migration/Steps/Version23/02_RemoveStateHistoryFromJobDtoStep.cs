using MongoDB.Bson;
using MongoDB.Driver;

namespace Hangfire.Mongo.Migration.Steps.Version23
{
    /// <summary>
    /// Removes the StateHistory field from JobDto documents after migration to separate collection.
    /// 
    /// Note: Expired jobs and their associated state history records are automatically cleaned up by MongoExpirationManager,
    /// which deletes both JobGraph entries and their corresponding StateHistory entries when jobs expire.
    /// This ensures that orphaned state history records do not accumulate over time.
    /// </summary>
    internal class RemoveStateHistoryFromJobDtoStep : IMongoMigrationStep
    {
        public MongoSchema TargetSchema => MongoSchema.Version23;
        public long Sequence => 2; // Runs after state history migration (Sequence 1)

        public bool Execute(IMongoDatabase database, MongoStorageOptions storageOptions, IMongoMigrationContext migrationContext)
        {
            var jobGraphCollection = database.GetCollection<BsonDocument>(storageOptions.Prefix + ".jobGraph");

            // Filter for JobDto documents
            var filter = Builders<BsonDocument>.Filter.Eq("_t", new BsonArray { "BaseJobDto", "ExpiringJobDto", "JobDto" });

            // Unset (remove) the StateHistory field
            var update = Builders<BsonDocument>.Update.Unset("StateHistory");

            jobGraphCollection.UpdateMany(filter, update);

            return true;
        }
    }
}