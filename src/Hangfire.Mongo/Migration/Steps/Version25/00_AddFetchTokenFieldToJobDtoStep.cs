using MongoDB.Bson;
using MongoDB.Driver;

namespace Hangfire.Mongo.Migration.Steps.Version25
{
    /// <summary>
    /// Adds FetchToken field (BsonNull) to all JobDto documents so that pre-upgrade jobs are
    /// readable by the new RemoveFromQueue CAS. Idempotent — skips documents that already have
    /// the field.
    /// </summary>
    internal class AddFetchTokenFieldToJobDtoStep : IMongoMigrationStep
    {
        public MongoSchema TargetSchema => MongoSchema.Version25;
        public long Sequence => 0;

        public bool Execute(IMongoDatabase database, MongoStorageOptions storageOptions, IMongoMigrationContext migrationContext)
        {
            var jobGraphCollection = database.GetCollection<BsonDocument>(storageOptions.Prefix + ".jobGraph");

            var filterWithoutFetchToken = Builders<BsonDocument>.Filter.And(
                Builders<BsonDocument>.Filter.Eq("_t", new BsonArray { "BaseJobDto", "ExpiringJobDto", "JobDto" }),
                Builders<BsonDocument>.Filter.Not(Builders<BsonDocument>.Filter.Exists("FetchToken"))
            );

            var setFetchTokenNull = Builders<BsonDocument>.Update.Set("FetchToken", BsonNull.Value);
            jobGraphCollection.UpdateMany(filterWithoutFetchToken, setFetchTokenNull);

            return true;
        }
    }
}
