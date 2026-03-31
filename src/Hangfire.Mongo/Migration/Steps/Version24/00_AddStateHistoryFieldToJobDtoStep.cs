using MongoDB.Bson;
using MongoDB.Driver;

namespace Hangfire.Mongo.Migration.Steps.Version24
{
    /// <summary>
    /// Adds StateHistory field (empty array) to all JobDto documents to prepare them for migration back from separate collection.
    /// This step is idempotent - it will not overwrite existing StateHistory fields.
    /// </summary>
    internal class AddStateHistoryFieldToJobDtoStep : IMongoMigrationStep
    {
        public MongoSchema TargetSchema => MongoSchema.Version24;
        public long Sequence => 0;

        public bool Execute(IMongoDatabase database, MongoStorageOptions storageOptions, IMongoMigrationContext migrationContext)
        {
            var jobGraphCollection = database.GetCollection<BsonDocument>(storageOptions.Prefix + ".jobGraph");

            // Filter for JobDto documents that don't have StateHistory yet
            var filterWithoutStateHistory = Builders<BsonDocument>.Filter.And(
                Builders<BsonDocument>.Filter.Eq("_t", new BsonArray { "BaseJobDto", "ExpiringJobDto", "JobDto" }),
                Builders<BsonDocument>.Filter.Not(Builders<BsonDocument>.Filter.Exists("StateHistory"))
            );

            var updateAdd = Builders<BsonDocument>.Update.Set("StateHistory", new BsonArray());
            jobGraphCollection.UpdateMany(filterWithoutStateHistory, updateAdd);

            return true;
        }
    }
}

