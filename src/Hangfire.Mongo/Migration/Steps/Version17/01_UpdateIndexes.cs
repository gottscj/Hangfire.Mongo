using MongoDB.Bson;
using MongoDB.Driver;

namespace Hangfire.Mongo.Migration.Steps.Version17
{
    internal class UpdateIndexes : IndexMigration, IMongoMigrationStep
    {
        public MongoSchema TargetSchema => MongoSchema.Version17;

        public long Sequence => 1;

        public bool Execute(IMongoDatabase database, MongoStorageOptions storageOptions, IMongoMigrationContext migrationContext)
        {
            var jobGraph = database.GetCollection<BsonDocument>($@"{storageOptions.Prefix}.jobGraph");
            var indexBuilder = Builders<BsonDocument>.IndexKeys;

            TryCreateIndexes(jobGraph, indexBuilder.Ascending, "Score");

            return true;
        }

    }

}
