using System.Linq;
using MongoDB.Bson;
using MongoDB.Driver;

namespace Hangfire.Mongo.Migration.Steps.Version24
{
    /// <summary>
    /// Drops the stateHistory collection after all state history has been migrated back to JobDto documents.
    /// This step is idempotent - it will succeed even if the collection doesn't exist.
    /// </summary>
    internal class DropStateHistoryCollectionStep : IMongoMigrationStep
    {
        public MongoSchema TargetSchema => MongoSchema.Version24;
        public long Sequence => 2; // Runs after state history migration (Sequence 1)

        public bool Execute(IMongoDatabase database, MongoStorageOptions storageOptions, IMongoMigrationContext migrationContext)
        {
            var collectionName = storageOptions.Prefix + ".stateHistory";

            // Check if collection exists
            var filter = new BsonDocument("name", collectionName);
            var collections = database.ListCollections(new ListCollectionsOptions { Filter = filter }).ToList();

            if (collections.Any())
            {
                database.DropCollection(collectionName);
            }

            return true;
        }
    }
}

