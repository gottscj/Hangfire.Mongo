using MongoDB.Bson;
using MongoDB.Driver;
using System;

namespace Hangfire.Mongo.Migration.Steps.Version09
{
    /// <summary>
    /// Create signal capped collection
    /// </summary>
    internal class CreateSignalCollection : IMongoMigrationStep
    {
        public MongoSchema TargetSchema => MongoSchema.Version09;

        public long Sequence => 0;

        public bool Execute(IMongoDatabase database, MongoStorageOptions storageOptions, IMongoMigrationContext migrationContext)
        {
            var name = $@"{storageOptions.Prefix}.signal";

            database.DropCollection(name);

            var cosmosStorageOptions = storageOptions as CosmosStorageOptions;
            if (cosmosStorageOptions != null)
            {
                database.CreateCollection(name);
                var collection = database.GetCollection<BsonDocument>(name);
                var options = new CreateIndexOptions { ExpireAfter = TimeSpan.FromHours(cosmosStorageOptions.CosmosHourlyTtl) };
                var field = new StringFieldDefinition<BsonDocument>("_ts");
                var indexDefinition = new IndexKeysDefinitionBuilder<BsonDocument>().Ascending(field);
                collection.Indexes.CreateOne(indexDefinition, options);
            }

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
