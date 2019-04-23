using System.Linq;
using MongoDB.Bson;
using MongoDB.Driver;

namespace Hangfire.Mongo.Migration.Steps.Version06
{
    /// <summary>
    /// Migrate enqueued jobs
    /// </summary>
    internal class CountersMigration : IMongoMigrationStep
    {
        public MongoSchema TargetSchema => MongoSchema.Version06;

        public long Sequence => 5;

        public bool Execute(IMongoDatabase database, MongoStorageOptions storageOptions, IMongoMigrationContext migrationContext)
        {
            // Update jobQueue to reflect new job id
            var stateDataCollection = database.GetCollection<BsonDocument>($@"{storageOptions.Prefix}.statedata");
            MigrateCounters(database, storageOptions, stateDataCollection);
            return true;
        }
        
        private static void MigrateCounters(IMongoDatabase database, MongoStorageOptions storageOptions, IMongoCollection<BsonDocument> stateData)
        {
            var counterCollection = database.GetCollection<BsonDocument>($@"{storageOptions.Prefix}.counter");
            var filter = Builders<BsonDocument>.Filter.Empty;
            var counters = counterCollection.Find(filter).ToList().Select(s =>
            {
                s["_t"] = new BsonArray(new[] { "KeyValueDto", "ExpiringKeyValueDto", "CounterDto" });
                return s;
            }).ToList();

            var aggregatedCounterCollection = database.GetCollection<BsonDocument>($@"{storageOptions.Prefix}.aggregatedcounter");
            var aggregatedCounters = aggregatedCounterCollection.Find(filter).ToList().Select(s =>
            {
                s["_t"] = new BsonArray(new[] { "KeyValueDto", "ExpiringKeyValueDto", "AggregatedCounterDto" });
                return s;
            }).ToList();

            foreach (var counter in counters)
            {
                var index = aggregatedCounters.FindIndex(a => a["Key"].AsString == counter["Key"].AsString);
                if (index < 0)
                {
                    continue;
                }
                counter["Value"] = counter["Value"].AsInt32 + aggregatedCounters[index]["Value"].AsInt64;
                aggregatedCounters.RemoveAt(index);
            }
            
            if (counters.Any())
            {
                stateData.InsertMany(counters);
            }

            if (aggregatedCounters.Any())
            {
                stateData.InsertMany(aggregatedCounters);
            }
        }

    }
}
