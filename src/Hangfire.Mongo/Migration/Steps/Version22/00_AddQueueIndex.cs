using MongoDB.Bson;
using MongoDB.Driver;

namespace Hangfire.Mongo.Migration.Steps.Version22;

internal class AddQueueIndex : IndexMigration, IMongoMigrationStep
{
    public MongoSchema TargetSchema => MongoSchema.Version22;
    public long Sequence => 0;
    public bool Execute(IMongoDatabase database, MongoStorageOptions storageOptions, IMongoMigrationContext migrationContext)
    {
        var jobGraph = database.GetCollection<BsonDocument>($"{storageOptions.Prefix}.jobGraph");

        // "{ _t: 1, Queue: 1 }"
        var index = Builders<BsonDocument>.IndexKeys.Ascending("_t").Ascending("Queue");
        var options = new CreateIndexOptions { Name = "T_Queue" };

        jobGraph.Indexes.CreateOne(index, options);
        return true;
    }
}