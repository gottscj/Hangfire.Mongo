using MongoDB.Bson;
using MongoDB.Driver;

namespace Hangfire.Mongo.Migration.Steps.Version20
{
    internal class CompoundIndexes : IndexMigration, IMongoMigrationStep
    {
        public MongoSchema TargetSchema => MongoSchema.Version20;
        public long Sequence => 1;
        public bool Execute(IMongoDatabase database, MongoStorageOptions storageOptions, IMongoMigrationContext migrationContext)
        {
            var jobGraph = database.GetCollection<BsonDocument>($@"{storageOptions.Prefix}.jobGraph");

            // "{ SetType: 1, _t: 1, Score: 1 }, { name: \"IX_SetType_T_Score\", background: true }"
            var index = Builders<BsonDocument>.IndexKeys.Ascending("SetType").Ascending("_t").Ascending("Score");
            var options = new CreateIndexOptions { Background = true, Name = "IX_SetType_T_Score" };
            
            // "{ _t: 1, ExpireAt: 1 }, {name: \"T_ExpireAt\", background: true}"
            var index2 = Builders<BsonDocument>.IndexKeys.Ascending("_t").Ascending("ExpireAt");
            var options2 = new CreateIndexOptions { Background = true, Name = "T_ExpireAt" };

            jobGraph.Indexes.CreateOne(index, options);
            jobGraph.Indexes.CreateOne(index2, options2);
            return true;
        }
    }
}