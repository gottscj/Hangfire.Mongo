using MongoDB.Driver;

namespace Hangfire.Mongo.Migration.Steps.Version06
{
    /// <summary>
    /// Create index for statedate collection
    /// </summary>
    internal class StateDataRename : IMongoMigrationStep
    {
        public MongoSchema TargetSchema => MongoSchema.Version6;

        public long Sequence => 0;

        public bool Execute(IMongoDatabase database, MongoStorageOptions storageOptions)
        {
            var oldName = $@"{storageOptions.Prefix}.statedata";
            var newName = $@"{storageOptions.Prefix}.stateData";
            database.RenameCollection(oldName, newName);
            return true;
        }
    }
}
