using System;
using System.Collections.Generic;
using System.Linq;
using MongoDB.Bson;
using MongoDB.Driver;

namespace Hangfire.Mongo.Migration.Steps.Version15
{
    internal class RemoveMergedCounters : IMongoMigrationStep
    {
        public MongoSchema TargetSchema => MongoSchema.Version15;

        public long Sequence => 3;
        
        public bool Execute(IMongoDatabase database, MongoStorageOptions storageOptions, IMongoMigrationBag migrationBag)
        {
            var jobGraph = database.GetCollection<BsonDocument>(storageOptions.Prefix + ".jobGraph");

            var counters = jobGraph.Find(new BsonDocument("_t", "CounterDto")).ToList();
            var idsToRemove = new BsonArray();
            foreach (var countersByKey in counters.GroupBy(c => c["Key"].AsString))
            {
                var key = countersByKey.Key;
                var groupedCounters = countersByKey.ToList();
               
                // if only one, nothing to do, continue...
                if (groupedCounters.Count == 1)
                {
                    continue;
                }
                
                // if all have the same value take the newest
                var allSameValue = groupedCounters.Select(c => Convert.ToInt32(c["Value"])).Distinct().Count() == 1;
                if (allSameValue)
                {
                    var newestObjectId = groupedCounters.Select(c => c["_id"].AsObjectId).Max();
                    idsToRemove.AddRange(groupedCounters.Where(c => c["_id"].AsObjectId != newestObjectId).Select(c => c["_id"]));
                    continue;
                }

                // if more with different values delete all with value = '1' and sum the rest, most likely there have been
                // created a new counterDto, which will have been counted instead of the aggregated one.
                idsToRemove.AddRange(groupedCounters.Where(c => Convert.ToInt32(c["Value"]) == 1).Select(c => c["_id"]));

                // verify there is only one counter left. if more, sum the results and put in a new document,
                // delete the existing
                groupedCounters.RemoveAll(c => idsToRemove.Contains(c["_id"].AsObjectId));

                if (groupedCounters.Count <= 1)
                {
                    continue;
                }

                var sum = groupedCounters.Sum(c =>
                {
                    var value = c["Value"];
                    return value.IsInt32 ? value.AsInt32 : value.AsInt64;
                });

                var expireAt = groupedCounters.Any(c => c.Contains("ExpireAt") && c["ExpireAt"] != BsonNull.Value)
                    ? (BsonValue) groupedCounters.Select(c => c["ExpireAt"].ToUniversalTime()).Max()
                    : BsonNull.Value;

                var counterToInsert = new BsonDocument
                {
                    ["Key"] = key,
                    ["Value"] = sum,
                    ["_id"] = ObjectId.GenerateNewId(),
                    ["ExpireAt"] = expireAt,
                    ["_t"] = new BsonArray(new[] {"BaseJobDto", "ExpiringJobDto", "KeyJobDto", "CounterDto"})
                };
                jobGraph.InsertOne(counterToInsert);
                idsToRemove.AddRange(groupedCounters.Select(c => c["_id"]));
            }

            if (!idsToRemove.Any())
            {
                return true;
            }
            
            jobGraph.DeleteMany(new BsonDocument("_id", new BsonDocument("$in", idsToRemove)));
            return true;
        }
    }
}