using MongoDB.Bson;
using MongoDB.Driver;
using System;

namespace Hangfire.Mongo.Migration.Steps.Version17
{
    internal class AddNotificationsCollection : IMongoMigrationStep
    {
        public MongoSchema TargetSchema { get; } = MongoSchema.Version17;
        public long Sequence { get; } = 0;
        
        public bool Execute(IMongoDatabase database, MongoStorageOptions storageOptions, IMongoMigrationContext migrationContext)
        {
            var cosmosStorageOptions = storageOptions as CosmosStorageOptions;
            if (cosmosStorageOptions != null)
            {
                database.CreateCollection(storageOptions.Prefix + ".notifications");
                var collection = database.GetCollection<BsonDocument>(storageOptions.Prefix + ".notifications");
                var options = new CreateIndexOptions { ExpireAfter = TimeSpan.FromHours(cosmosStorageOptions.CosmosHourlyTtl) };
                var field = new StringFieldDefinition<BsonDocument>("_ts");
                var indexDefinition = new IndexKeysDefinitionBuilder<BsonDocument>().Ascending(field);
                collection.Indexes.CreateOne(indexDefinition, options);
            }

            database.CreateCollection(storageOptions.Prefix + ".notifications", new CreateCollectionOptions
            {
                Capped = true,
                MaxSize = 1048576 * 16, // 16 MB,
                MaxDocuments = 100000,
            });

            return true;
        }
    }
}