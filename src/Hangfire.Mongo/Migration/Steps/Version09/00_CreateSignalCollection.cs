using MongoDB.Bson;
using MongoDB.Driver;

namespace Hangfire.Mongo.Migration.Steps.Version08
{
    /// <summary>
    /// Create signal capped collection
    /// </summary>
    internal class CreateSignalCollection : IMongoMigrationStep
    {
        public MongoSchema TargetSchema => MongoSchema.Version9;

        public long Sequence => 0;

        public bool Execute(IMongoDatabase database, MongoStorageOptions storageOptions, IMongoMigrationBag migrationBag)
        {
            var name = $@"{storageOptions.Prefix}.signal";

            var createOptions = new CreateCollectionOptions
            {
                Capped = true,
                MaxSize = 1000000,
                MaxDocuments = 1000
            };
            database.CreateCollection(name, createOptions);

            // We have to create one document to get the trailing curser working
            var dummySignal = new BsonDocument
            {
                ["Signaled"] = "false",
                ["Name"] = "dummy"
            };

            database.GetCollection<BsonDocument>(name).InsertOne(dummySignal);

            return true;
        }
    }
}
