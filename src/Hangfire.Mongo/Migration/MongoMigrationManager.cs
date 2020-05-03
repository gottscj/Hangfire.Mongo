using System;
using System.Linq;
using System.Reflection;
using Hangfire.Mongo.Dto;
using Hangfire.Mongo.Migration.Strategies;
using Hangfire.Mongo.Migration.Strategies.Backup;
using MongoDB.Driver;

namespace Hangfire.Mongo.Migration
{

    /// <summary>
    /// Manages migration from one schema version to the required.
    /// </summary>
    internal class MongoMigrationManager
    {
        private readonly MongoStorageOptions _storageOptions;
        private readonly IMongoDatabase _database;
 
        public static MongoSchema RequiredSchemaVersion =>
            Enum.GetValues(typeof(MongoSchema)).Cast<MongoSchema>().OrderBy(v => v).Last();


        internal MongoMigrationManager(MongoStorageOptions storageOptions, IMongoDatabase database)
        {
            _storageOptions = storageOptions;
            _database = database;
        }

        public static bool MigrateIfNeeded(MongoStorageOptions storageOptions, IMongoDatabase database)
        {
            using (var migrationLock = new MigrationLock(database, storageOptions.Prefix, storageOptions.MigrationLockTimeout))
            {
                var migrationManager = new MongoMigrationManager(storageOptions, database);
                migrationLock.AcquireMigrationAccess();
                return migrationManager.Migrate(storageOptions.MigrationOptions.BackupStrategy, storageOptions.MigrationOptions.MigrationStrategy);
            }
        }

        private bool Migrate(MongoBackupStrategy backupStrategy, MongoMigrationStrategy migrationStrategy)
        {
            var currentSchema = _database
                .GetCollection<SchemaDto>(_storageOptions.Prefix + ".schema")
                .Find(_ => true)
                .FirstOrDefault();
            
            if (currentSchema == null)
            {
                // We do not have a schema version yet
                // - assume an empty database and run full migrations
                currentSchema = new SchemaDto
                {
                    Version = MongoSchema.None
                };
            }

            if (RequiredSchemaVersion < currentSchema.Version)
            {
                var assemblyName = GetType().GetTypeInfo().Assembly.GetName();
                throw new InvalidOperationException(
                    $"{Environment.NewLine}{assemblyName.Name} version: {assemblyName.Version}, uses a schema prior to the current database." +
                    $"{Environment.NewLine}Backwards migration is not supported. Please resolve this manually (e.g. by dropping the database)." +
                    $"{Environment.NewLine}Please see https://github.com/sergeyzwezdin/Hangfire.Mongo#migration for further information.");
            }

            if (RequiredSchemaVersion == currentSchema.Version)
            {
                // Nothing to migrate - so let's get outta here.
                return false;
            }

            if (backupStrategy == null)
            {
                throw new InvalidOperationException(
                    $"{Environment.NewLine}You need to choose a migration strategy by setting the {nameof(MongoStorageOptions)}.{nameof(MongoStorageOptions.MigrationOptions)} property." +
                    $"{Environment.NewLine}Please see https://github.com/sergeyzwezdin/Hangfire.Mongo#migration for further information.");
            }
            
            backupStrategy
                .Backup(_storageOptions, _database, currentSchema.Version, RequiredSchemaVersion);
            
            migrationStrategy
                .Execute(_storageOptions, _database, currentSchema.Version, RequiredSchemaVersion);

            return true;
        }
    }
}
