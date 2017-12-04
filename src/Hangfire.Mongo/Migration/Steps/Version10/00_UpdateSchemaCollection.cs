using System;
using MongoDB.Bson;
using MongoDB.Driver;

namespace Hangfire.Mongo.Migration.Steps.Version10
{
    /// <summary>
    /// Update the schema collection adding a database identifier
    /// </summary>
    internal class UpdateSchemaCollection : IMongoMigrationStep
    {
        public MongoSchema TargetSchema => MongoSchema.Version10;

        public long Sequence => 0;

        public bool Execute(IMongoDatabase database, MongoStorageOptions storageOptions, IMongoMigrationBag migrationBag)
        {
            var name = $@"{storageOptions.Prefix}.schema";

            var filter = Builders<BsonDocument>.Filter.Not(Builders<BsonDocument>.Filter.Exists("Identifier"));
            var update = Builders<BsonDocument>.Update.Set("Identifier", Guid.NewGuid().ToString());

            database
                .GetCollection<BsonDocument>(name)
                .FindOneAndUpdate(filter, update);

            return true;
        }
    }
}
