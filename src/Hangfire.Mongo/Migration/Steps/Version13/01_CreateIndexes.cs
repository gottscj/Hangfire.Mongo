using MongoDB.Bson;
using MongoDB.Driver;

namespace Hangfire.Mongo.Migration.Steps.Version13
{
    internal class CreateIndexes : IndexMigration, IMongoMigrationStep
    {
        public MongoSchema TargetSchema => MongoSchema.Version13;
        public long Sequence => 1;
        
        public bool Execute(IMongoDatabase database, MongoStorageOptions storageOptions, IMongoMigrationBag migrationBag)
        {
            var indexBuilder = Builders<BsonDocument>.IndexKeys;
            
            var jobGraphCollection = database.GetCollection<BsonDocument>(storageOptions.Prefix + ".jobGraph");
            TryCreateIndexes(jobGraphCollection, indexBuilder.Descending, "StateName", "ExpireAt", "_t", "Queue",
                "FetchedAt", "Value");
            
            TryCreateIndexes(jobGraphCollection, indexBuilder.Ascending, "Key");
            
            var locksCollection = database.GetCollection<BsonDocument>($@"{storageOptions.Prefix}.locks");
            TryCreateIndexes(locksCollection, indexBuilder.Descending, "Resource", "ExpireAt");
            
            var serverCollection = database.GetCollection<BsonDocument>($@"{storageOptions.Prefix}.server");
            TryCreateIndexes(serverCollection, indexBuilder.Descending, "LastHeartbeat");
            
            return true;
        }
    }
}