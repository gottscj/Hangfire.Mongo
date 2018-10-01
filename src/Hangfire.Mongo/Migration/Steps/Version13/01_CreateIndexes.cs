using MongoDB.Bson;
using MongoDB.Driver;

namespace Hangfire.Mongo.Migration.Steps.Version13
{
    internal class CreateIndexes : IMongoMigrationStep
    {
        public MongoSchema TargetSchema => MongoSchema.Version13;
        public long Sequence => 1;
        
        public bool Execute(IMongoDatabase database, MongoStorageOptions storageOptions, IMongoMigrationBag migrationBag)
        {
            var indexBuilder = Builders<BsonDocument>.IndexKeys;
            
            var jobGraphCollection = database.GetCollection<BsonDocument>(storageOptions.Prefix + ".jobGraph");
            jobGraphCollection.TryCreateIndexes(indexBuilder.Descending, "StateName", "ExpireAt", "_t", "Queue",
                "FetchedAt", "Value");
            jobGraphCollection.TryCreateIndexes(indexBuilder.Ascending, "Key");
            
            var locksCollection = database.GetCollection<BsonDocument>($@"{storageOptions.Prefix}.locks");
            locksCollection.TryCreateIndexes(indexBuilder.Descending, "Resource", "ExpireAt");
            
            var serverCollection = database.GetCollection<BsonDocument>($@"{storageOptions.Prefix}.server");
            serverCollection.TryCreateIndexes(indexBuilder.Descending, "LastHeartbeat");
            
            return true;
        }
    }
}