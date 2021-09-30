using MongoDB.Driver;

namespace Hangfire.Mongo.Migration.Steps.Version17
{
    internal class AddNotificationsCollection : IMongoMigrationStep
    {
        public MongoSchema TargetSchema { get; } = MongoSchema.Version17;
        public long Sequence { get; } = 0;
        
        public bool Execute(IMongoDatabase database, MongoStorageOptions storageOptions, IMongoMigrationContext migrationContext)
        {
            storageOptions.CreateNotificationsCollection(database);
            return true;
        }
    }
}