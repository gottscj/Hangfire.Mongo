using MongoDB.Driver;

namespace Hangfire.Mongo.Migration.Steps.Version16
{
    internal class RemoveObsoleteCollections : IMongoMigrationStep
    {
        public MongoSchema TargetSchema { get; } = MongoSchema.Version16;
        public long Sequence { get; } = 99;
        
        public bool Execute(IMongoDatabase database, MongoStorageOptions storageOptions, IMongoMigrationBag migrationBag)
        {
            database.DropCollection(storageOptions.Prefix + ".signal");
            return true;
        }
    }
}