using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Driver;

namespace Hangfire.Mongo.Migration.Steps.Version19
{
    internal class AddTypeToSetDto : IndexMigration, IMongoMigrationStep
    {
        public MongoSchema TargetSchema => MongoSchema.Version19;
        public long Sequence => 0;
        public bool Execute(IMongoDatabase database, MongoStorageOptions storageOptions, IMongoMigrationContext migrationContext)
        {
            var jobGraph = database.GetCollection<BsonDocument>($@"{storageOptions.Prefix}.jobGraph");
            var indexBuilder = Builders<BsonDocument>.IndexKeys;

            var cursor = jobGraph
                .FindSync(new BsonDocument("_t", "SetDto"));

            while (cursor.MoveNext())
            {
                Parallel.ForEach(cursor.Current, doc =>
                {
                    var key = doc["Key"].AsString;
                    var index = key?.IndexOf("<");
                    if (!(index > 0))
                    {
                        return;
                    }
                    var type = key.Substring(0, index.Value);
                    jobGraph
                        .UpdateOne(new BsonDocument("_id", doc["_id"]), 
                            new BsonDocument("$set", new BsonDocument("SetType", type)));
                });
            }
            
            TryCreateIndexes(jobGraph, indexBuilder.Ascending, "SetType");
            return true;
        }
    }
}