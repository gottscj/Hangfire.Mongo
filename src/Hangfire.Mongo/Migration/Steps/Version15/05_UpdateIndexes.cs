using MongoDB.Bson;
using MongoDB.Driver;

namespace Hangfire.Mongo.Migration.Steps.Version15
{
    internal class UpdateIndexes : IndexMigration, IMongoMigrationStep
    {
        public MongoSchema TargetSchema => MongoSchema.Version15;

        public long Sequence => 5;

        public bool Execute(IMongoDatabase database, MongoStorageOptions storageOptions, IMongoMigrationBag migrationBag)
        {
            var jobGraph = database.GetCollection<BsonDocument>($@"{storageOptions.Prefix}.jobGraph");
            var indexBuilder = Builders<BsonDocument>.IndexKeys;
            
            TryCreateUniqueIndexes(jobGraph, indexBuilder.Ascending, "Key");
            TryCreateIndexes(jobGraph, indexBuilder.Descending, "StateName", "ExpireAt", "_t", "Queue",
                "FetchedAt", "Value", "Item");
           
            return true;
        }
        
    }
}