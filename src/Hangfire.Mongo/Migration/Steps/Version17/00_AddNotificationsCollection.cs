using MongoDB.Driver;

namespace Hangfire.Mongo.Migration.Steps.Version17
{
    internal class AddNotificationsCollection : IMongoMigrationStep
    {
        public MongoSchema TargetSchema { get; } = MongoSchema.Version17;
        public long Sequence { get; } = 0;

        public bool Execute(IMongoDatabase database, MongoStorageOptions storageOptions,
            IMongoMigrationContext migrationContext)
        {
            if (!storageOptions.SupportsCappedCollection)
            {
                return true;
            }

            database.CreateCollection(storageOptions.Prefix + ".notifications", new CreateCollectionOptions
            {
                Capped = true,
                MaxSize = 1048576 * 16, // 16 MB,
                MaxDocuments = 100000
            });
            return true;
        }
    }
}