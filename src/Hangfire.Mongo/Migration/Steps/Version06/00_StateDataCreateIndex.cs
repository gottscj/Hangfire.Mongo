using MongoDB.Bson;
using MongoDB.Driver;

namespace Hangfire.Mongo.Migration.Steps.Version06
{
    /// <summary>
    /// Create index for statedate collection
    /// </summary>
    internal class StateDataCreateIndex : IMongoMigrationStep
    {
        public MongoSchema TargetSchema => MongoSchema.Version06;

        public long Sequence => 0;

        public bool Execute(IMongoDatabase database, MongoStorageOptions storageOptions, IMongoMigrationBag migrationBag)
        {
            var indexBuilder = Builders<BsonDocument>.IndexKeys;

            var statedataCollection = database.GetCollection<BsonDocument>($@"{storageOptions.Prefix}.statedata");
            statedataCollection.TryCreateIndexes(indexBuilder.Ascending, "Key");

            return true;
        }
    }
}
