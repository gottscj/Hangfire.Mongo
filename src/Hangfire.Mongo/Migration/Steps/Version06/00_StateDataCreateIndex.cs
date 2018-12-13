using MongoDB.Bson;
using MongoDB.Driver;

namespace Hangfire.Mongo.Migration.Steps.Version06
{
    /// <summary>
    /// Create index for statedate collection
    /// </summary>
    internal class StateDataCreateIndex : IndexMigration, IMongoMigrationStep
    {
        public MongoSchema TargetSchema => MongoSchema.Version06;

        public long Sequence => 0;

        public bool Execute(IMongoDatabase database, MongoStorageOptions storageOptions, IMongoMigrationBag migrationBag)
        {
            var indexBuilder = Builders<BsonDocument>.IndexKeys;

            var statedataCollection = database.GetCollection<BsonDocument>($@"{storageOptions.Prefix}.statedata");
            TryCreateIndexes(statedataCollection, indexBuilder.Ascending, "Key");

            return true;
        }
    }
}
