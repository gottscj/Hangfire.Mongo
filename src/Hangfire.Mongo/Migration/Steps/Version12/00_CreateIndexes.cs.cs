using MongoDB.Bson;
using MongoDB.Driver;

namespace Hangfire.Mongo.Migration.Steps.Version12
{
    /// <summary>
    /// Automatically create indexes
    /// </summary>
    internal class CreateIndexes : IndexMigration, IMongoMigrationStep
    {

        public MongoSchema TargetSchema => MongoSchema.Version12;

        public long Sequence => 0;

        public bool Execute(IMongoDatabase database, MongoStorageOptions storageOptions, IMongoMigrationBag migrationBag)
        {
            var indexBuilder = Builders<BsonDocument>.IndexKeys;

            var jobQueueCollection = database.GetCollection<BsonDocument>($@"{storageOptions.Prefix}.jobQueue");
            TryCreateIndexes(jobQueueCollection, indexBuilder.Descending, "Queue", "FetchedAt");

            var jobCollection = database.GetCollection<BsonDocument>($@"{storageOptions.Prefix}.job");
            TryCreateIndexes(jobCollection, indexBuilder.Descending, "StateName", "ExpireAt");

            var stateDataCollection = database.GetCollection<BsonDocument>($@"{storageOptions.Prefix}.stateData");
            TryCreateIndexes(stateDataCollection, indexBuilder.Descending, "ExpireAt", "_t");

            var locksCollection = database.GetCollection<BsonDocument>($@"{storageOptions.Prefix}.locks");
            TryCreateIndexes(locksCollection, indexBuilder.Descending, "Resource", "ExpireAt");

            var serverCollection = database.GetCollection<BsonDocument>($@"{storageOptions.Prefix}.server");
            TryCreateIndexes(serverCollection, indexBuilder.Descending, "LastHeartbeat");

            var signalCollection = database.GetCollection<BsonDocument>($@"{storageOptions.Prefix}.signal");
            TryCreateIndexes(signalCollection, indexBuilder.Descending, "Signaled");

            return true;
        }

    }
}
