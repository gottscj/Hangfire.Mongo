using System;
using System.Linq;
using System.Reflection;
using Hangfire.Mongo.Dto;
using Hangfire.Mongo.Migration.Strategies;
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
        private readonly MongoMigrationRunner _migrationRunner;
        private readonly IMongoCollection<SchemaDto> _schemas;
 
        public static MongoSchema RequiredSchemaVersion =>
            Enum.GetValues(typeof(MongoSchema)).Cast<MongoSchema>().OrderBy(v => v).Last();


        public MongoMigrationManager(MongoStorageOptions storageOptions, IMongoDatabase database)
        {
            _storageOptions = storageOptions;
            _database = database;
            _schemas = _database.GetCollection<SchemaDto>(storageOptions.Prefix + ".schema");
            _migrationRunner = new MongoMigrationRunner(database, storageOptions, _schemas);  
        }

        public static void MigrateIfNeeded(MongoStorageOptions storageOptions, IMongoDatabase database)
        {
            var migrateLockCollectionName = storageOptions.Prefix + ".migrationLock";
            using (new MigrationLock(database, migrateLockCollectionName, storageOptions.MigrationLockTimeout))
            {
                var migrationManager = new MongoMigrationManager(storageOptions, database);
                migrationManager.Migrate();
            }
        }
        
        public void Migrate()
        {
            var currentSchema = _schemas.Find(_ => true).FirstOrDefault();
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
                return;
            }

            IMongoMigrationStrategy migration;
            switch (_storageOptions.MigrationOptions.Strategy)
            {
                case MongoMigrationStrategy.None:
                    migration = new MongoMigrationStrategyNone();
                    break;
                case MongoMigrationStrategy.Drop:
                    migration = new MongoMigrationStrategyDrop(_database, _storageOptions, _migrationRunner);
                    break;
                case MongoMigrationStrategy.Migrate:
                    migration = new MongoMigrationStrategyMigrate(_database, _storageOptions, _migrationRunner);
                    break;
                default:
                    throw new ArgumentOutOfRangeException(
                        $@"Unknown migration strategy: {_storageOptions.MigrationOptions.Strategy}",
                        $@"{nameof(MongoMigrationOptions)}.{nameof(MongoMigrationOptions.Strategy)}");
            }

            migration.Execute(currentSchema.Version, RequiredSchemaVersion);
        }
    }
}
