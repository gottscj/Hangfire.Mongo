using MongoDB.Bson;
using MongoDB.Driver;

namespace Hangfire.Mongo.Migration.Steps.Version07
{
    /// <summary>
    /// Create index for statedate collection
    /// </summary>
    internal class StateDataRename : IMongoMigrationStep
    {
        public MongoSchema TargetSchema => MongoSchema.Version7;

        public long Sequence => 0;

        public bool Execute(IMongoDatabase database, MongoStorageOptions storageOptions, IMongoMigrationBag migrationBag)
        {
            var oldName = $@"{storageOptions.Prefix}.statedata";
            var newName = $@"{storageOptions.Prefix}.stateData";

            var options = new ListCollectionsOptions
            {
                Filter = new FilterDefinitionBuilder<BsonDocument>().Eq("name", oldName)
            };

            if (database.ListCollections(options).Any())
            {
                database.RenameCollection(oldName, newName);
            }
            return true;
        }
    }
}
