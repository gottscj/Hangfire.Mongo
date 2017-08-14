using System;
using System.Linq;
using Hangfire.Mongo.Database;

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

        public override void Migrate(MongoSchema fromSchema, MongoSchema toSchema)
        {
            var existingCollectionNames = ExistingHangfireCollectionNameSpaces(fromSchema).ToList();
            if (!_storageOptions.MigrationOptions.Backup)
            {
                foreach (var collectionName in existingCollectionNames)
                {
                    _dbContext.Database.DropCollection(collectionName);
                }
            }
            else
            {
                // Backup was requested. 
                var backupCollectionNames =
                    existingCollectionNames.ToDictionary(k => k, v => BackupCollectionName(v, toSchema));

                // Let's double check that we have not backed up before.
                foreach (var collectionName in ExistingDatabaseCollectionNames())
                {
                    if (backupCollectionNames.Values.ToList().Contains(collectionName))
                    {
                        throw new InvalidOperationException(
                            $"{Environment.NewLine}{collectionName} already exists. Cannot perform backup." +
                            $"{Environment.NewLine}Cannot overwrite existing backups. Please resolve this manually (e.g. by droping collection)." +
                            $"{Environment.NewLine}Please see https://github.com/sergeyzwezdin/Hangfire.Mongo#migration for further information.");
                    }
                }

                // Now do the actual back (by renaming hangfire collections)
                foreach (var collection in backupCollectionNames)
                {
                    _dbContext.Database.RenameCollection(collection.Key, collection.Value);
                }
            }

            // Now the database should be cleared for all hangfire collections
            // - assume an empty database and run full migrations
            var migrationRunner = new MongoMigrationRunner(_dbContext, _storageOptions);
            migrationRunner.Execute(MongoSchema.None, toSchema);
        }



    }
}
