using MongoDB.Driver;

namespace Hangfire.Mongo.Migration.Steps.Version23
{
    internal class CreateStateHistoryCollectionMigrationStep : IMongoMigrationStep
    {
        public MongoSchema TargetSchema => MongoSchema.Version23;
        public long Sequence => 0;

        public bool Execute(IMongoDatabase database, MongoStorageOptions storageOptions, IMongoMigrationContext migrationContext)
        {
            var collectionName = storageOptions.Prefix + ".stateHistory";
            
            // Check if collection already exists to avoid errors
            var filter = new MongoDB.Bson.BsonDocument("name", collectionName);
            var collections = database.ListCollections(new ListCollectionsOptions { Filter = filter });

            if (!collections.Any())
            {
                database.CreateCollection(collectionName);
                
                // Create index on JobId field for efficient querying
                var collection = database.GetCollection<MongoDB.Bson.BsonDocument>(collectionName);
                var indexKeysDefinition = Builders<MongoDB.Bson.BsonDocument>.IndexKeys.Ascending("JobId");
                var indexOptions = new CreateIndexOptions { Name = "IX_JobId" };
                collection.Indexes.CreateOne(new CreateIndexModel<MongoDB.Bson.BsonDocument>(indexKeysDefinition, indexOptions));
            }

            return true;
        }
    }
}