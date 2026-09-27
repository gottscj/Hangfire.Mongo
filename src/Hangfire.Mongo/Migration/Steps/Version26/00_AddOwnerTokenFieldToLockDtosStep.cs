using Hangfire.Mongo.Dto;
using MongoDB.Bson;
using MongoDB.Driver;

namespace Hangfire.Mongo.Migration.Steps.Version26
{
    /// <summary>
    /// Adds OwnerToken fields to existing lock documents without replacing tokens already assigned.
    /// </summary>
    internal class AddOwnerTokenFieldToLockDtosStep : IMongoMigrationStep
    {
        public MongoSchema TargetSchema => MongoSchema.Version26;
        public long Sequence => 0;

        public bool Execute(IMongoDatabase database, MongoStorageOptions storageOptions, IMongoMigrationContext migrationContext)
        {
            AddOwnerToken(database.GetCollection<BsonDocument>(storageOptions.Prefix + ".locks"));
            AddOwnerToken(database.GetCollection<BsonDocument>(storageOptions.Prefix + ".migrationLock"));
            return true;
        }

        private static void AddOwnerToken(IMongoCollection<BsonDocument> collection)
        {
            var missingOwnerToken = Builders<BsonDocument>.Filter.Not(
                Builders<BsonDocument>.Filter.Exists("OwnerToken"));
            var setOwnerTokenNull = Builders<BsonDocument>.Update.Set("OwnerToken", BsonNull.Value);

            collection.UpdateMany(missingOwnerToken, setOwnerTokenNull);
        }
    }
}