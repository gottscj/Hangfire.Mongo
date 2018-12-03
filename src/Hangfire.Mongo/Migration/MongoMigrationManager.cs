using System;
using System.Linq;
using System.Reflection;
using Hangfire.Mongo.Database;
using Hangfire.Mongo.DistributedLock;
using Hangfire.Mongo.Dto;
using Hangfire.Mongo.Migration.Strategies;
using MongoDB.Driver;

namespace Hangfire.Mongo.Migration
{

    /// <summary>
    /// Manages migration from one schema version to the required.
    /// </summary>
    internal class MongoMigrationManager : IDisposable
    {
        private readonly MongoStorageOptions _storageOptions;
        private readonly HangfireDbContext _dbContext;
        private readonly MongoMigrationRunner _migrationRunner;
        private readonly MongoDistributedLock _lock;
        private readonly string _migrateLockCollectionName = "migrationLock";
        
        public static MongoSchema RequiredSchemaVersion => Enum.GetValues(typeof(MongoSchema)).Cast<MongoSchema>().OrderBy(v => v).Last();
        
        
        public MongoMigrationManager(MongoStorageOptions storageOptions, HangfireDbContext dbContext)
        {
            _storageOptions = storageOptions;
            _dbContext = dbContext;
            _migrationRunner = new MongoMigrationRunner(dbContext, storageOptions);
            var migrateLockCollection = _dbContext.Database.GetCollection<DistributedLockDto>(_migrateLockCollectionName);
            _lock = new MongoDistributedLock(nameof(Migrate), TimeSpan.FromSeconds(30), migrateLockCollection,
                _storageOptions);
        }

        public void Migrate()
        {
            var currentSchema = _dbContext.Schema.Find(_ => true).FirstOrDefault();
            if (currentSchema == null)
            {
                // We do not have a schema version yet
                // - assume an empty database and run full migrations
                _migrationRunner.Execute(MongoSchema.None, RequiredSchemaVersion);
                return;
            }

            if (RequiredSchemaVersion < currentSchema.Version)
            {
                var assemblyName = GetType().GetTypeInfo().Assembly.GetName();
                throw new InvalidOperationException(
                    $"{Environment.NewLine}{assemblyName.Name} version: {assemblyName.Version}, uses a schema prior to the current database." +
                    $"{Environment.NewLine}Backwards migration is not supported. Please resolve this manually (e.g. by droping the database)." +
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
                    migration = new MongoMigrationStrategyDrop(_dbContext, _storageOptions, _migrationRunner);
                    break;
                case MongoMigrationStrategy.Migrate:
                    migration = new MongoMigrationStrategyMigrate(_dbContext, _storageOptions, _migrationRunner);
                    break;
                default:
                    throw new ArgumentOutOfRangeException(
                        $@"Unknown migration strategy: {_storageOptions.MigrationOptions.Strategy}",
                        $@"{nameof(MongoMigrationOptions)}.{nameof(MongoMigrationOptions.Strategy)}");
            }

            migration.Execute(currentSchema.Version, RequiredSchemaVersion);
        }


        public void Dispose()
        {
            _lock.Dispose();
            _dbContext.Database.DropCollection(_migrateLockCollectionName);
        }
    }
}
