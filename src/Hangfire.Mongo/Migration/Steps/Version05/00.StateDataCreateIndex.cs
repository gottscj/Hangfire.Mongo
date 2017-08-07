using MongoDB.Bson;
using MongoDB.Driver;

namespace Hangfire.Mongo.Migration.Steps.Version05
{
    /// <summary>
    /// Create index for statedate collection
    /// </summary>
    internal class StateDataCreateIndex : IMongoMigrationStep
    {
        public MongoSchema TargetSchema => MongoSchema.Version5;

        public long Sequence => 0;

        public bool Execute(IMongoDatabase database, MongoStorageOptions storageOptions)
        {
            var db = database.GetCollection<BsonDocument>($@"{storageOptions.Prefix}.statedata");
            var index = new BsonDocumentIndexKeysDefinition<BsonDocument>(new BsonDocument("Key", 1));
            db.Indexes.CreateOne(index);
            return true;
        }
    }
}
