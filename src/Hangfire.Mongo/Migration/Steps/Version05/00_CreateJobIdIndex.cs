using MongoDB.Bson;
using MongoDB.Driver;

namespace Hangfire.Mongo.Migration.Steps.Version05
{
    /// <summary>
    /// Create index for statedate collection
    /// </summary>
    internal class CreateJobIdIndex : IndexMigration, IMongoMigrationStep
    {
        public MongoSchema TargetSchema => MongoSchema.Version05;

        public long Sequence => 0;

        public bool Execute(IMongoDatabase database, MongoStorageOptions storageOptions, IMongoMigrationBag migrationBag)
        {
            var indexBuilder = Builders<BsonDocument>.IndexKeys;

            var jobParameterCollection = database.GetCollection<BsonDocument>($@"{storageOptions.Prefix}.jobParameter");
            TryCreateIndexes(jobParameterCollection, indexBuilder.Descending, "JobId");

            var jobQueueCollection = database.GetCollection<BsonDocument>($@"{storageOptions.Prefix}.jobQueue");
            
            TryCreateIndexes(jobQueueCollection, indexBuilder.Descending, "JobId");

            var stateCollection = database.GetCollection<BsonDocument>($@"{storageOptions.Prefix}.state");
            TryCreateIndexes(stateCollection, indexBuilder.Descending, "JobId");

            return true;
        }

    }
}
