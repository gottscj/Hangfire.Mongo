using System.Collections.Generic;
using System.Linq;
using MongoDB.Bson;
using MongoDB.Driver;

namespace Hangfire.Mongo.Migration.Steps.Version13
{
    internal class SumCounters : IMongoMigrationStep
    {
        public MongoSchema TargetSchema => MongoSchema.Version13;
        public long Sequence => 2;
        
        public bool Execute(IMongoDatabase database, MongoStorageOptions storageOptions, IMongoMigrationBag migrationBag)
        {
            var jobGraph = database.GetCollection<BsonDocument>(storageOptions.Prefix + ".jobGraph");
            
            var counterFilter= new BsonDocument("_t", "CounterDto");
            var counters = jobGraph.FindSync(counterFilter).ToList();
            if (!counters.Any())
            {
                return true;
            }
            var countersToInsert = new List<BsonDocument>();
            foreach (var countersByKey in counters.GroupBy(c => c["Key"].AsString))
            {
                var key = countersByKey.Key;
                var sum = countersByKey.Sum(c => c["Value"].AsInt64);
                BsonValue expireAt = BsonNull.Value;
                if (countersByKey.Any(c =>  c.Contains("ExpireAt") && c["ExpireAt"] != BsonNull.Value))
                {
                    expireAt = countersByKey
                        .Where(c => c.Contains("ExpireAt"))
                        .Select(c => c["ExpireAt"].ToUniversalTime())
                        .Max();
                }

                var counterToInsert = new BsonDocument
                {
                    ["Key"] = key,
                    ["Value"] = sum,
                    ["_id"] = ObjectId.GenerateNewId(),
                    ["ExpireAt"] = expireAt,
                    ["_t"] = new BsonArray(new[] {"BaseJobDto", "ExpiringJobDto", "KeyJobDto", "CounterDto"})
                };
                countersToInsert.Add(counterToInsert);
            }
            
            jobGraph.InsertMany(countersToInsert);

            return true;
        }
    }
}