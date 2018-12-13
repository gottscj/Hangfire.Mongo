using Hangfire.Mongo.Database;
using MongoDB.Driver;

namespace Hangfire.Mongo.Migration.Strategies
{
    /// <summary>
    /// Implements the "Drop" strategy.
    /// Drops all hangfire related collections.
    /// If backup is enabled, the collections will be renamed instead.
    /// </summary>
    internal class MongoMigrationStrategyDrop : MongoMigrationStrategyBase
    {

        public MongoMigrationStrategyDrop(HangfireDbContext dbContext, MongoStorageOptions storageOptions, MongoMigrationRunner migrationRunner)
            : base(dbContext, storageOptions, migrationRunner)
        {
        }


        protected override void Migrate(MongoSchema fromSchema, MongoSchema toSchema)
        {
            base.Migrate(MongoSchema.None, toSchema);
        }


        protected override void BackupStrategyNone(IMongoDatabase database, MongoSchema fromSchema, MongoSchema toSchema)
        {
            DropHangfireCollections(database, fromSchema);
        }


        protected override void BackupCollection(IMongoDatabase database, string collectionName, string backupCollectionName)
        {
            database.RenameCollection(collectionName, backupCollectionName);
        }


        protected override void BackupStrategyDatabase(IMongoClient client, IMongoDatabase database, MongoSchema fromSchema, MongoSchema toSchema)
        {
            base.BackupStrategyDatabase(client, database, fromSchema, toSchema);

            // Now the database has been copied,
            // drop  the hangfire collections.
            DropHangfireCollections(database, fromSchema);
        }


        private void DropHangfireCollections(IMongoDatabase database, MongoSchema schema)
        {
            foreach (var collectionName in ExistingHangfireCollectionNames(schema))
            {
                database.DropCollection(collectionName);
            }
        }
    }
}
