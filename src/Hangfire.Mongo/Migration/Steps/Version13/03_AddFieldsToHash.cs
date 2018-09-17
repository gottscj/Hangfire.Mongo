using System.Collections.Generic;
using System.Linq;
using MongoDB.Bson;
using MongoDB.Driver;

namespace Hangfire.Mongo.Migration.Steps.Version13
{
    internal class AddFieldsToHash : IMongoMigrationStep
    {
        public MongoSchema TargetSchema => MongoSchema.Version13;
        public long Sequence => 3;
        public bool Execute(IMongoDatabase database, MongoStorageOptions storageOptions, IMongoMigrationBag migrationBag)
        {
            var jobGraph = database.GetCollection<BsonDocument>(storageOptions.Prefix + ".jobGraph");
            
            var hashFilter= new BsonDocument("_t", "HashDto");
            var hashItems = jobGraph.FindSync(hashFilter).ToList();
            if (!hashItems.Any())
            {
                return true;
            }
            var hashItemsToInsert = new List<BsonDocument>();
            foreach (var hashItemsByKey in hashItems.GroupBy(c => c["Key"].AsString))
            {
                var key = hashItemsByKey.Key;

                var fields = new BsonDocument();
                foreach (var hash in hashItemsByKey)
                {
                    fields[hash["Field"].AsString] = hash["Value"].AsString;
                }
                // some fields don't have 'ExpireAt' field set from previous migrations.
                // fix the offense by adding it now.
                BsonValue expireAt = BsonNull.Value;
                if (hashItemsByKey.Any(c => c.Contains("ExpireAt") && c["ExpireAt"] != BsonNull.Value))
                {
                    expireAt = hashItemsByKey.Max(c => c["ExpireAt"].ToUniversalTime());
                }

                var toInsert = new BsonDocument
                {
                    ["Key"] = key,
                    ["Fields"] = fields,
                    ["_id"] = ObjectId.GenerateNewId(),
                    ["ExpireAt"] = expireAt,
                    ["_t"] = new BsonArray(new[] {"BaseJobDto", "ExpiringJobDto", "KeyJobDto", "HashDto"})
                };
                hashItemsToInsert.Add(toInsert);
            }
            
            jobGraph.InsertMany(hashItemsToInsert);

            return true;
        }
    }
}