using MongoDB.Bson;
using MongoDB.Driver;

namespace Hangfire.Mongo.Migration.Steps.Version14
{
    internal class RemoveLegacyLocks : IMongoMigrationStep
    {
        public MongoSchema TargetSchema => MongoSchema.Version14;

        public long Sequence => 1;
        
        public bool Execute(IMongoDatabase database, MongoStorageOptions storageOptions, IMongoMigrationBag migrationBag)
        {
            var locksCollection = database.GetCollection<BsonDocument>(storageOptions.Prefix + ".locks");           
            locksCollection.DeleteMany(new BsonDocument("ClientId", new BsonDocument("$exists", true)));
            
            return true;
        }
    }
}