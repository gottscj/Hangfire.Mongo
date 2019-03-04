using System;
using System.Linq;
using System.Reflection;
using System.Threading;
using Hangfire.Logging;
using Hangfire.Mongo.Database;
using Hangfire.Mongo.DistributedLock;
using Hangfire.Mongo.Dto;
using Hangfire.Mongo.Migration.Strategies;
using Hangfire.Storage;
using MongoDB.Bson;
using MongoDB.Driver;

namespace Hangfire.Mongo.Migration
{

    /// <summary>
    /// Manages migration from one schema version to the required.
    /// </summary>
    internal class MongoMigrationManager : IDisposable
    {
        private static readonly ILog Logger = LogProvider.For<MongoMigrationManager>();

        private readonly MongoStorageOptions _storageOptions;
        private readonly HangfireDbContext _dbContext;
        private readonly MongoMigrationRunner _migrationRunner;

        private readonly string _migrateLockCollectionName = "migrationLock";

        public static MongoSchema RequiredSchemaVersion =>
            Enum.GetValues(typeof(MongoSchema)).Cast<MongoSchema>().OrderBy(v => v).Last();


        public MongoMigrationManager(MongoStorageOptions storageOptions, HangfireDbContext dbContext)
        {
            _storageOptions = storageOptions;
            _dbContext = dbContext;
            _migrationRunner = new MongoMigrationRunner(dbContext, storageOptions);

            AcquireMigrationAccess();
        }
        
        
        public void Migrate()
        {
            var currentSchema = _dbContext.Schema.Find(_ => true).FirstOrDefault();
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
            _dbContext.Database.DropCollection(_migrateLockCollectionName);
        }

        private void AcquireMigrationAccess()
        {
            try
            {
                var migrationLock = _dbContext.Database.GetCollection<MigrationLockDto>(_migrateLockCollectionName);
                // If result is null, then it means we acquired the lock
                var isLockAcquired = false;
                var now = DateTime.Now;
                // wait maximum 5 seconds for migration to complete
                var lockTimeoutTime = now.Add(_storageOptions.MigrationLockTimeout);

                while (!isLockAcquired && (lockTimeoutTime >= now))
                {
                    // Acquire the lock if it does not exist - Notice: ReturnDocument.Before
                    var filter = new BsonDocument(nameof(MigrationLockDto.Lock), "locked");
                    var update = Builders<MigrationLockDto>.Update.SetOnInsert(_ => _.Lock, "locked");
                    var options = new FindOneAndUpdateOptions<MigrationLockDto>
                    {
                        IsUpsert = true,
                        ReturnDocument = ReturnDocument.Before
                    };

                    try
                    {
                        var result = migrationLock.FindOneAndUpdate(filter, update, options);

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
                            now = DateTime.Now;
                        }
                    }
                    catch (MongoCommandException)
                    {
                        // this can occur if two processes attempt to acquire a lock on the same resource simultaneously.
                        // unfortunately there doesn't appear to be a more specific exception type to catch.
                        Thread.Sleep(20);
                        now = DateTime.Now;
                    }
                }

                if (!isLockAcquired)
                {
                    throw new InvalidOperationException("Could not complete migration in 5 seconds");
                }
            }
            catch (InvalidOperationException)
            {
                throw;
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException(
                    "Could not complete migration in 5 seconds': Check inner exception for details.", ex);
            }
        }
    }
}
