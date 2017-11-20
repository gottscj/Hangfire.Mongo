using MongoDB.Driver;

namespace Hangfire.Mongo.Migration.Steps.Version09
{
    /// <summary>
    /// Create signal capped collection
    /// </summary>
    internal class CreateSignalCollection : IMongoMigrationStep
    {
        public MongoSchema TargetSchema => MongoSchema.Version09;

        public long Sequence => 0;

        public bool Execute(IMongoDatabase database, MongoStorageOptions storageOptions, IMongoMigrationBag migrationBag)
        {
            var name = $@"{storageOptions.Prefix}.signal";

            database.DropCollection(name);

            var createOptions = new CreateCollectionOptions
            {
                Capped = true,
                MaxSize = 1000000,
                MaxDocuments = 1000
            };
            database.CreateCollection(name, createOptions);

            return true;
        }
    }
}
