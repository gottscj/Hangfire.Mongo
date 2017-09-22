using MongoDB.Bson;
using MongoDB.Driver;

namespace Hangfire.Mongo.Migration.Steps.Version05
{
    /// <summary>
    /// Create index for statedate collection
    /// </summary>
    internal class CreateJobIdIndex : IMongoMigrationStep
    {
        public MongoSchema TargetSchema => MongoSchema.Version05;

        public long Sequence => 0;

        public bool Execute(IMongoDatabase database, MongoStorageOptions storageOptions, IMongoMigrationBag migrationBag)
        {
            CreateJobIndex(database.GetCollection<BsonDocument>($@"{storageOptions.Prefix}.jobParameter"));
            CreateJobIndex(database.GetCollection<BsonDocument>($@"{storageOptions.Prefix}.jobQueue"));
            CreateJobIndex(database.GetCollection<BsonDocument>($@"{storageOptions.Prefix}.state"));
            return true;
        }

        private static void CreateJobIndex(IMongoCollection<BsonDocument> collection)
        {
            var index = new BsonDocumentIndexKeysDefinition<BsonDocument>(new BsonDocument("JobId", -1));
            var options = new CreateIndexOptions
            {
                Name = "JobId",
            };
            collection.Indexes.CreateOne(index, options);
        }

    }
}
