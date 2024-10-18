using MongoDB.Bson;
using MongoDB.Driver;

namespace Hangfire.Mongo.Migration.Steps.Version21
{
    internal class AddIndexesMigration : IndexMigration, IMongoMigrationStep
    {
        public MongoSchema TargetSchema => MongoSchema.Version21;
        public long Sequence => 0;
        public bool Execute(IMongoDatabase database, 
            MongoStorageOptions storageOptions, 
            IMongoMigrationContext migrationContext)
        {
            var jobGraph = database.GetCollection<BsonDocument>($@"{storageOptions.Prefix}.jobGraph");

            // "{ Queue: 1, _t: 1, FetchedAt: 1 }"
            var index = Builders<BsonDocument>.IndexKeys.Ascending("Queue").Ascending("_t").Ascending("FetchedAt");
            var options = new CreateIndexOptions { Name = "Queue_T_FetchedAt" };
            
            // "{ Key: 1, _t: 1 }"
            var index2 = Builders<BsonDocument>.IndexKeys.Ascending("Key").Ascending("_t");
            var options2 = new CreateIndexOptions { Background = true, Name = "Key_T" };

            jobGraph.Indexes.CreateOne(index, options);
            jobGraph.Indexes.CreateOne(index2, options2);
            return true;
        }
    }
}