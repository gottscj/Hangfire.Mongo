using System.Linq;
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

        public MongoMigrationStrategyDrop(HangfireDbContext dbContext, MongoStorageOptions storageOptions)
            : base(dbContext, storageOptions)
        {
        }


        protected override void BackupStrategyNone(IMongoDatabase database, MongoSchema fromSchema, MongoSchema toSchema)
        {
            var existingCollectionNames = ExistingHangfireCollectionNames(fromSchema).ToList();
            foreach (var collectionName in existingCollectionNames)
            {
                _dbContext.Database.DropCollection(collectionName);
            }
        }


        protected override void BackupCollection(IMongoDatabase database, string collectionName, string backupCollectionName)
        {
            database.RenameCollection(collectionName, backupCollectionName);
        }

    }
}
