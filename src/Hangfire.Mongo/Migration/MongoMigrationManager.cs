using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Hangfire.Mongo.Database;
using Hangfire.Mongo.DistributedLock;
using MongoDB.Bson;
using MongoDB.Driver;

namespace Hangfire.Mongo.Migration
{

    /// <summary>
    /// All the hangfire mongo schema versions ever used
    /// </summary>
    internal enum MongoSchema
    {
        None = 0,
        Version5 = 5,
        Version6 = 6,
    }


    /// <summary>
    /// Helpers for MongoSchema
    /// </summary>
    internal static class MongoSchemaExtentions
    {
        /// <summary>
        /// Is the schema version the last and current one.
        /// </summary>
        internal static bool IsLast(this MongoSchema schema)
        {
            return Enum.GetValues(typeof(MongoSchema))
                       .OfType<MongoSchema>()
                       .Last() == schema;
        }

        /// <summary>
        /// Get the next schema version.
        /// </summary>
        /// <returns>The next.</returns>
        /// <param name="schema">Schema.</param>
        internal static MongoSchema Next(this MongoSchema schema)
        {
            var schemaValues = Enum.GetValues(typeof(MongoSchema))
                                   .OfType<MongoSchema>()
                                   .ToList();
            return schemaValues.ElementAt(schemaValues.IndexOf(schema));
        }
    }


    /// <summary>
    /// Manages migration from one schema version to the required.
    /// </summary>
    internal class MongoMigrationManager
    {
        private readonly MongoStorageOptions _storageOptions;

        public static MongoSchema RequiredSchemaVersion => Enum.GetValues(typeof(MongoSchema)).Cast<MongoSchema>().Last();

        public MongoMigrationManager(MongoStorageOptions storageOptions)
        {
            _storageOptions = storageOptions;
        }

        public void Migrate(HangfireDbContext dbContext)
        {
            using (new MongoDistributedLock(nameof(Migrate), TimeSpan.FromSeconds(30), dbContext, _storageOptions))
            {

                var currentSchema = dbContext.Schema.Find(new BsonDocument()).FirstOrDefault();
                if (currentSchema == null)
                {
                    // We do not have a schema version yet
                    // - assume an empty database and run full migrations
                    var migrationRunner = new MongoMigrationRunner(dbContext, _storageOptions);
                    migrationRunner.Execute(MongoSchema.None, RequiredSchemaVersion);
                    return;
                }

                var currentSchemaVersion = (MongoSchema)currentSchema.Version;
                if (RequiredSchemaVersion < currentSchemaVersion)
                {
                    var assemblyName = GetType().GetTypeInfo().Assembly.GetName();
                    throw new InvalidOperationException(
                        $"{Environment.NewLine}{assemblyName.Name} version: {assemblyName.Version}, uses a schema prior to the current database." +
                        $"{Environment.NewLine}Backwards migration is not supported. Please resolve this manually (e.g. by droping the database)." +
                        $"{Environment.NewLine}Please see https://github.com/sergeyzwezdin/Hangfire.Mongo#migration for further information.");
                }

                if (RequiredSchemaVersion > currentSchemaVersion)
                {
                    switch (_storageOptions.MigrationOptions.Strategy)
                    {
                        case MongoMigrationStrategy.None:
                            ExecuteNone();
                            break;
                        case MongoMigrationStrategy.Drop:
                            ExecuteDrop(dbContext, currentSchemaVersion);
                            break;
                        case MongoMigrationStrategy.Migrate:
                            ExecuteMigrate(dbContext, currentSchemaVersion);
                            break;
                        default:
                            throw new ArgumentOutOfRangeException($@"Unknown migration strategy: {_storageOptions.MigrationOptions.Strategy}", nameof(_storageOptions.MigrationOptions.Strategy));
                    }
                }
            }
        }


        /// <summary>
        /// Executes the none strategy.
        /// Well, not much to execute just throw an exception.
        /// We do not want to continue on an obsolete schema.
        /// </summary>
        private void ExecuteNone()
        {
            var assemblyName = GetType().GetTypeInfo().Assembly.GetName();
            throw new InvalidOperationException(
                $"{Environment.NewLine}{assemblyName.Name} version: {assemblyName.Version}, introduces a new schema version that requires migration." +
                $"{Environment.NewLine}You can choose a migration strategy by setting the {nameof(MongoStorageOptions)}.{nameof(MongoStorageOptions.MigrationOptions)} property." +
                $"{Environment.NewLine}Please see https://github.com/sergeyzwezdin/Hangfire.Mongo#migration for further information.");
        }


        /// <summary>
        /// Executes the drop strategy.
        /// Drops all hangfire related collections.
        /// If backup is enabled, the collections will be renamed instead.
        /// </summary>
        /// <param name="dbContext">Referance to the mongo database.</param>
        /// <param name="currentSchema">The schema currently used.</param>
        private void ExecuteDrop(HangfireDbContext dbContext, MongoSchema currentSchema)
        {
            foreach (var collection in ExistingHangfireCollectionNameSpaces(dbContext))
            {
                if (!_storageOptions.MigrationOptions.Backup)
                {
                    dbContext.Database.DropCollection(collection.CollectionName);
                    continue;
                }

                var backupCollectionName = BackupCollectionName(collection.CollectionName, currentSchema);
                try
                {
                    dbContext.Database.RenameCollection(collection.CollectionName, backupCollectionName);
                }
                catch (MongoCommandException e)
                {
                    // According to this document:
                    // https://github.com/mongodb/mongo/blob/master/src/mongo/base/error_codes.err
                    // The backup collection already exists.
                    if (e.Code == 48)
                    {
                        // Assume that the collection is already backed up - just drop the original
                        dbContext.Database.DropCollection(collection.CollectionName);
                        continue;
                    }
                    throw;
                }
            }

            // Now the database should be cleared for all hangfire collections
            // - assume an empty database and run full migrations
            var migrationRunner = new MongoMigrationRunner(dbContext, _storageOptions);
            migrationRunner.Execute(MongoSchema.None, RequiredSchemaVersion);
        }


        private void ExecuteMigrate(HangfireDbContext dbContext, MongoSchema currentSchema)
        {
            if (_storageOptions.MigrationOptions.Backup)
            {
                throw new NotImplementedException($@"Backup for migration step '{nameof(MongoMigrationStrategy.Migrate)}' is not yet implemented");
                //foreach (var collection in ExistingHangfireCollectionNameSpaces(dbContext))
                //{
                //    BackupCollection(dbContext, collection.CollectionName, currentSchema);
                //}
            }

            var migrationRunner = new MongoMigrationRunner(dbContext, _storageOptions);
            migrationRunner.Execute(currentSchema, RequiredSchemaVersion);
        }


        /// <summary>
        /// Find hangfire collection namespaces by reflecting over properties on database.
        /// </summary>
        private IEnumerable<CollectionNamespace> HangfireCollectionNameSpaces(HangfireDbContext database)
        {
            return database.GetType().GetTypeInfo().GetProperties().Where(prop =>
            {
                var typeInfo = prop.PropertyType.GetTypeInfo();
                return typeInfo.IsGenericType &&
                       typeof(IMongoCollection<>).GetTypeInfo().IsAssignableFrom(typeInfo.GetGenericTypeDefinition());
            }).Select(prop =>
            {
                dynamic collection = prop.GetValue(database);
                return collection.CollectionNamespace as CollectionNamespace;
            });
        }


        /// <summary>
        /// Find hangfire collection namespaces by reflecting over properties on database.
        /// </summary>
        private IEnumerable<CollectionNamespace> ExistingHangfireCollectionNameSpaces(HangfireDbContext database)
        {
            var collectionNamespaces = HangfireCollectionNameSpaces(database);

            var existingCollectionNames = database.Database.ListCollections()
                                              .ToList()
                                              .Select(c => c["name"]).ToList();

            return collectionNamespaces.Where(c => existingCollectionNames.Contains(c.CollectionName));
        }


        /// <summary>
        /// Backups the collection in database identified by collectionName.
        /// </summary>
        /// <param name="database">Referance to the mongo database.</param>
        /// <param name="collectionName">The name of the collection to backup.</param>
        /// <param name="currentSchema">The schema currently used.</param>
        private void BackupCollection(HangfireDbContext database, string collectionName, MongoSchema currentSchema)
        {
            var backupCollectionName = BackupCollectionName(collectionName, currentSchema);
            var dbSource = database.Database.GetCollection<BsonDocument>(collectionName);
            var indexes = dbSource.Indexes.List().ToList().Where(idx => idx["name"] != "_id_").ToList();
            if (indexes.Any())
            {
                var dbBackup = database.Database.GetCollection<BsonDocument>(backupCollectionName);
                foreach (var index in indexes)
                {
                    var newIndex = new BsonDocumentIndexKeysDefinition<BsonDocument>(index);
                    dbBackup.Indexes.CreateOne(newIndex, new CreateIndexOptions());
                }
            }
            var aggDoc = new Dictionary<string, object>
            {
                { "aggregate", collectionName},
                { "pipeline", new []
                    {
                        new Dictionary<string, object> { { "$match", new BsonDocument() } },
                        new Dictionary<string, object> { { "$out", backupCollectionName } }
                    }
                }
            };

            var doc = new BsonDocument(aggDoc);
            var command = new BsonDocumentCommand<BsonDocument>(doc);
            database.Database.RunCommand(command);
        }


        private string BackupCollectionName(string collectionName, MongoSchema schema)
        {
            return $@"{collectionName}.{(int)schema}.{_storageOptions.MigrationOptions.BackupPostfix}";
        }

    }
}
