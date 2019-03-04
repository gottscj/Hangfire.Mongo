using System;
using System.Linq;
using System.Reflection;
using System.Threading;
using Hangfire.Logging;
using Hangfire.Mongo.DistributedLock;
using Hangfire.Mongo.Dto;
using Hangfire.Mongo.Migration.Strategies;
using MongoDB.Bson;
using MongoDB.Driver;

namespace Hangfire.Mongo.Migration
{

    /// <summary>
    /// Manages migration from one schema version to the required.
    /// </summary>
    internal class MongoMigrationManager
    {
        private static readonly ILog Logger = LogProvider.For<MongoMigrationManager>();

        private readonly MongoStorageOptions _storageOptions;
        private readonly IMongoDatabase _database;
        private readonly MongoMigrationRunner _migrationRunner;

        private readonly string _migrateLockCollectionName;
        private readonly IMongoCollection<SchemaDto> _schemas;

        private readonly BsonDocument _migrationIdFilter =
            new BsonDocument("_id", new BsonObjectId("5c351d07197a9bcdba4832fc"));
        
        public static MongoSchema RequiredSchemaVersion =>
            Enum.GetValues(typeof(MongoSchema)).Cast<MongoSchema>().OrderBy(v => v).Last();


        public MongoMigrationManager(MongoStorageOptions storageOptions, IMongoDatabase database)
        {
            _storageOptions = storageOptions;
            _database = database;
            _schemas = _database.GetCollection<SchemaDto>(storageOptions.Prefix + ".schema");
            _migrationRunner = new MongoMigrationRunner(database, storageOptions, _schemas);
            _migrateLockCollectionName = storageOptions.Prefix + ".migrationLock";
        }

        public static void MigrateIfNeeded(MongoStorageOptions storageOptions, IMongoDatabase database)
        {
            var migrationManager = new MongoMigrationManager(storageOptions, database);
            migrationManager.Migrate();
        }
        
        public void Migrate()
        {
            AcquireMigrationAccess();
            var currentSchema = _schemas.Find(_ => true).FirstOrDefault();
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

        private void AcquireMigrationAccess()
        {
            try
            {
                var migrationLock = _database.GetCollection<MigrationLockDto>(_migrateLockCollectionName);
                
                // If result is null, then it means we acquired the lock
                var isLockAcquired = false;
                var now = DateTime.UtcNow;
                // wait maximum double of configured seconds for migration to complete
                var lockTimeoutTime = now.Add(_storageOptions.MigrationLockTimeout);

                var deleteFilter = new BsonDocument("$and", new BsonArray
                {
                    _migrationIdFilter,
                    new BsonDocument(nameof(MigrationLockDto.ExpireAt), new BsonDocument("$lt", DateTime.UtcNow))
                });

                migrationLock.DeleteOne(deleteFilter);
                
                // busy wait
                while (!isLockAcquired && (lockTimeoutTime >= now))
                {
                    // Acquire the lock if it does not exist - Notice: ReturnDocument.Before
                    var update = Builders<MigrationLockDto>
                        .Update
                        .SetOnInsert(_ => _.ExpireAt, lockTimeoutTime);
                    
                    var options = new FindOneAndUpdateOptions<MigrationLockDto>
                    {
                        IsUpsert = true,
                        ReturnDocument = ReturnDocument.Before
                    };

                    try
                    {
                        var result = migrationLock.FindOneAndUpdate(_migrationIdFilter, update, options);

                        // If result is null, it means we acquired the lock
                        if (result == null)
                        {
                            if (Logger.IsDebugEnabled())
                            {
                                Logger.Debug("Acquired lock for migration");
                            }

                            isLockAcquired = true;
                        }
                        else
                        {
                            Thread.Sleep(20);
                            now = DateTime.UtcNow;
                        }
                    }
                    catch (MongoCommandException)
                    {
                        // this can occur if two processes attempt to acquire a lock on the same resource simultaneously.
                        // unfortunately there doesn't appear to be a more specific exception type to catch.
                        Thread.Sleep(20);
                        now = DateTime.UtcNow;
                    }
                }

                if (!isLockAcquired)
                {
                    throw new InvalidOperationException("Could not complete migration. Never acquired lock");
                }
            }
            catch (InvalidOperationException)
            {
                throw;
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException(
                    "Could not complete migration: Check inner exception for details.", ex);
            }
        }
    }
}
