using MongoDB.Bson;
using MongoDB.Driver;

namespace Hangfire.Mongo.Migration.Steps.Version12
{
    /// <summary>
    /// Automatically create indexes
    /// </summary>
    internal class CreateIndexes : IMongoMigrationStep
    {

        public MongoSchema TargetSchema => MongoSchema.Version12;

        public long Sequence => 0;

        public bool Execute(IMongoDatabase database, MongoStorageOptions storageOptions, IMongoMigrationBag migrationBag)
        {
            var indexBuilder = Builders<BsonDocument>.IndexKeys;

            var jobQueueCollection = database.GetCollection<BsonDocument>($@"{storageOptions.Prefix}.jobQueue");
            jobQueueCollection.TryCreateIndexes(indexBuilder.Descending, "Queue", "FetchedAt");

            var jobCollection = database.GetCollection<BsonDocument>($@"{storageOptions.Prefix}.job");
            jobCollection.TryCreateIndexes(indexBuilder.Descending, "StateName", "ExpireAt");

            var stateDataCollection = database.GetCollection<BsonDocument>($@"{storageOptions.Prefix}.stateData");
            stateDataCollection.TryCreateIndexes(indexBuilder.Descending, "ExpireAt", "_t");

            var locksCollection = database.GetCollection<BsonDocument>($@"{storageOptions.Prefix}.locks");
            locksCollection.TryCreateIndexes(indexBuilder.Descending, "Resource", "ExpireAt");

            var serverCollection = database.GetCollection<BsonDocument>($@"{storageOptions.Prefix}.server");
            serverCollection.TryCreateIndexes(indexBuilder.Descending, "LastHeartbeat");

            var signalCollection = database.GetCollection<BsonDocument>($@"{storageOptions.Prefix}.signal");
            signalCollection.TryCreateIndexes(indexBuilder.Descending, "Signaled");

            return true;
        }

    }
}
