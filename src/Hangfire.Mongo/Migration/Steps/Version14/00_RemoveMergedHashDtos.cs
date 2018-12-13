using MongoDB.Bson;
using MongoDB.Driver;

namespace Hangfire.Mongo.Migration.Steps.Version14
{
    /// <summary>
    /// Removes HashDto's which should have been removed in Version13 -> 'AddFieldsToHash'
    /// </summary>
    internal class RemoveMergedHashDtos : IMongoMigrationStep
    {
        public MongoSchema TargetSchema => MongoSchema.Version14;
        public long Sequence => 0;
        
        public bool Execute(IMongoDatabase database, MongoStorageOptions storageOptions, IMongoMigrationBag migrationBag)
        {
            var jobGraph = database.GetCollection<BsonDocument>(storageOptions.Prefix + ".jobGraph");
            var filter = new BsonDocument("$and", new BsonArray
            {
                new BsonDocument("_t.3", "HashDto"),
                new BsonDocument("Field", new BsonDocument("$exists", true))
            });

            jobGraph.DeleteMany(filter);
            return true;
        }
    }
}