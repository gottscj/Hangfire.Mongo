using System.Collections.Generic;
using System.Linq;
using MongoDB.Bson;
using MongoDB.Driver;

namespace Hangfire.Mongo.Migration.Steps.Version06
{
    /// <summary>
    /// Migrate scheduled jobs
    /// </summary>
    internal class ScheduledJobMigration : IMongoMigrationStep
    {
        public MongoSchema TargetSchema => MongoSchema.Version06;

        public long Sequence => 2;

        public bool Execute(IMongoDatabase database, MongoStorageOptions storageOptions, IMongoMigrationBag migrationBag)
        {
            var jobIdMapping = migrationBag.GetItem<Dictionary<int, string>>("JobIdMapping");

            var setCollection = database.GetCollection<BsonDocument>($@"{storageOptions.Prefix}.set");
            var filter = Builders<BsonDocument>.Filter.Eq("Key", "schedule");
            var migratedSetList = setCollection.Find(filter).ToList().Select(s =>
            {
                s["Value"] = jobIdMapping[int.Parse(s["Value"].AsString)];
                s["_t"] = new BsonArray(new[] { "KeyValueDto", "ExpiringKeyValueDto", "SetDto" });
                return s;
            }).ToList();

            if (migratedSetList.Any())
            {
                var stateDataCollection = database.GetCollection<BsonDocument>($@"{storageOptions.Prefix}.statedata");
                stateDataCollection.InsertMany(migratedSetList);
            }
            return true;
        }

    }
}
