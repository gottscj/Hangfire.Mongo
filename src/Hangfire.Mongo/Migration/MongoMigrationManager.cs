using System;
using System.Linq;
using System.Reflection;
using Hangfire.Mongo.Database;
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
        private readonly MongoStorageOptions _storageOptions;

        public static MongoSchema RequiredSchemaVersion => Enum.GetValues(typeof(MongoSchema)).Cast<MongoSchema>().OrderBy(v => v).Last();

        public MongoMigrationManager(MongoStorageOptions storageOptions)
        {
            _storageOptions = storageOptions ?? throw new ArgumentNullException(nameof(storageOptions));
        }

        public void Migrate(HangfireDbContext dbContext)
        {
            // Read the current schema without grabing the migrate lock.
            // Chances are that we are on par with the schema.
            var currentSchemaVersion = GetCurrentSchema(dbContext);
            if (RequiredSchemaVersion == currentSchemaVersion)
            {
                // Nothing to migrate - so let's get outahere.
                return;
            }

            // We must migrate - so grap the migrate lock and go...
            using (new MongoDistributedLock("migrate", TimeSpan.FromSeconds(30), dbContext, _storageOptions))
            {
                // Read the current schema one more time under lock.
                // It might have changed in the time from aquiring
                // the lock until we actually got it.
                currentSchemaVersion = GetCurrentSchema(dbContext);

                if (currentSchemaVersion == RequiredSchemaVersion)
                {
                    // Another migration has brought us up to par.
                    // We can safely return here.
                    return;
                }

                if (RequiredSchemaVersion < currentSchemaVersion)
                {
                    var assemblyName = GetType().GetTypeInfo().Assembly.GetName();
                    throw new InvalidOperationException(
                        $"{Environment.NewLine}{assemblyName.Name} version: {assemblyName.Version}, uses a schema prior to the current connection." +
                        $"{Environment.NewLine}Backwards migration is not supported. Please resolve this manually (e.g. by droping the connection)." +
                        $"{Environment.NewLine}Please see https://github.com/sergeyzwezdin/Hangfire.Mongo#migration for further information.");
                }

                if (currentSchemaVersion == MongoSchema.None)
                {
                    // We do not have a schema version yet
                    // - assume an empty database and run a full migration
                    var migrationRunner = new MongoMigrationRunner(dbContext, _storageOptions);
                    migrationRunner.Execute(MongoSchema.None, RequiredSchemaVersion);
                    return;
                }

                IMongoMigrationStrategy migration;
                switch (_storageOptions.MigrationOptions.Strategy)
                {
                    case MongoMigrationStrategy.None:
                        migration = new MongoMigrationStrategyNone();
                        break;
                    case MongoMigrationStrategy.Drop:
                        migration = new MongoMigrationStrategyDrop(dbContext, _storageOptions);
                        break;
                    case MongoMigrationStrategy.Migrate:
                        migration = new MongoMigrationStrategyMigrate(dbContext, _storageOptions);
                        break;
                    default:
                        throw new ArgumentOutOfRangeException($@"Unknown migration strategy: {_storageOptions.MigrationOptions.Strategy}", $@"{nameof(MongoMigrationOptions)}.{nameof(MongoMigrationOptions.Strategy)}");
                }

                migration.Execute(currentSchemaVersion, RequiredSchemaVersion);
            }
        }

        private MongoSchema GetCurrentSchema(HangfireDbContext dbContext)
        {
            var schema = dbContext.Schema.Find(new BsonDocument()).FirstOrDefault() ?? new SchemaDto();
            return schema.Version;
        }

    }
}
