using System.Linq;
using MongoDB.Bson;
using MongoDB.Driver;

namespace Hangfire.Mongo.Migration.Steps.Version06
{
    /// <summary>
    /// Migrate recurrent jobs
    /// </summary>
    internal class RecurringJobMigration : IMongoMigrationStep
    {
        public MongoSchema TargetSchema => MongoSchema.Version06;

        public long Sequence => 3;

        public bool Execute(IMongoDatabase database, MongoStorageOptions storageOptions, IMongoMigrationBag migrationBag)
        {
            var stateDataCollection = database.GetCollection<BsonDocument>($@"{storageOptions.Prefix}.statedata");
            MigrateHash(database, storageOptions, stateDataCollection);
            MigrateSet(database, storageOptions, stateDataCollection);
            return true;
        }


        private void MigrateHash(IMongoDatabase database, MongoStorageOptions storageOptions, IMongoCollection<BsonDocument> stateData)
        {
            var hashCollection = database.GetCollection<BsonDocument>($@"{storageOptions.Prefix}.hash");
            var filter = Builders<BsonDocument>.Filter.Regex("Key", "/^recurring-job:/");
            var migratedHashList = hashCollection.Find(filter).ToList().Select(s =>
            {
                s["_t"] = new BsonArray(new[] { "KeyValueDto", "ExpiringKeyValueDto", "HashDto" });
                return s;
            }).ToList();

            if (migratedHashList.Any())
            {
                stateData.InsertMany(migratedHashList);
            }
        }


        private void MigrateSet(IMongoDatabase database, MongoStorageOptions storageOptions, IMongoCollection<BsonDocument> stateData)
        {
            var setCollection = database.GetCollection<BsonDocument>($@"{storageOptions.Prefix}.set");
            var filter = Builders<BsonDocument>.Filter.Eq("Key", "recurring-jobs");
            var migratedSetList = setCollection.Find(filter).ToList().Select(s =>
            {
                s["_t"] = new BsonArray(new[] { "KeyValueDto", "ExpiringKeyValueDto", "SetDto" });
                return s;
            }).ToList();

            if (migratedSetList.Any())
            {
                stateData.InsertMany(migratedSetList);
            }
        }

    }
}
