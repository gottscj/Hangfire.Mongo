using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Hangfire.Mongo.Database;
using Hangfire.Mongo.DistributedLock;
using Hangfire.Mongo.Dto;
using MongoDB.Bson;
using MongoDB.Driver;

namespace Hangfire.Mongo.Migration
{

    internal enum MongoSchema
    {
        Version5 = 5,
        Version6 = 6
    }

    /// <summary>
    /// Manages migration from one schema version to the required
    /// </summary>
    internal class MongoMigrationManager
    {
        private readonly MongoStorageOptions _storageOptions;

        public static int RequiredSchemaVersion => Enum.GetValues(typeof(MongoSchema)).Cast<int>().Last();

        public MongoMigrationManager(MongoStorageOptions storageOptions)
        {
            _storageOptions = storageOptions;
        }

        public void Migrate(HangfireDbContext database)
        {
            using (new MongoDistributedLock(nameof(Migrate), TimeSpan.FromSeconds(30), database, _storageOptions))
            {
                var schema = database.Schema.Find(new BsonDocument()).FirstOrDefault();
                if (schema == null)
                {
                    // We do not have a schema version yet - assume an empty database.
                    // Write required schema version and return.
                    database.Schema.InsertOne(new SchemaDto { Version = RequiredSchemaVersion });
                    return;
                }

                if (RequiredSchemaVersion > schema.Version)
                {
                    switch (_storageOptions.MigrationOptions.Strategy)
                    {
                        case MongoMigrationStrategy.None:
                            ExecuteNone();
                            break;
                        case MongoMigrationStrategy.Drop:
                            ExecuteDrop(database, (MongoSchema)schema.Version);
                            break;
                        case MongoMigrationStrategy.Migrate:
                            ExecuteMigrate(database);
                            break;
                        default:
                            throw new ArgumentOutOfRangeException($@"Unknown migration strategy: {_storageOptions.MigrationOptions.Strategy}", nameof(_storageOptions.MigrationOptions.Strategy));
                    }
                }

                // Update the schema version to required version
                database.Schema.DeleteMany(_ => true);
                database.Schema.InsertOne(new SchemaDto { Version = RequiredSchemaVersion });
            }
        }


        private void ExecuteNone()
        {
            var assemblyName = GetType().GetTypeInfo().Assembly.GetName();
            throw new InvalidOperationException(
                $"{Environment.NewLine}{assemblyName.Name} version: {assemblyName.Version}, introduces a new schema version that requires migration." +
                $"{Environment.NewLine}You can choose a migration strategy by setting the {nameof(MongoStorageOptions)}.{nameof(MongoStorageOptions.MigrationOptions)} property." +
                $"{Environment.NewLine}Please see https://github.com/sergeyzwezdin/Hangfire.Mongo#migration for further information.");
        }


        private void ExecuteDrop(HangfireDbContext database, MongoSchema schema)
        {
            var collections = database.GetType().GetTypeInfo().GetProperties().Where(prop =>
            {
                var typeInfo = prop.PropertyType.GetTypeInfo();
                return typeInfo.IsGenericType &&
                       typeof(IMongoCollection<>).GetTypeInfo().IsAssignableFrom(typeInfo.GetGenericTypeDefinition());
            }).Select(prop =>
            {
                dynamic collection = prop.GetValue(database);
                return collection.CollectionNamespace as CollectionNamespace;
            });

            var existingCollectionNames = database.Database.ListCollections()
                                              .ToList()
                                              .Select(c => c["name"]).ToList();
            foreach (var collection in collections.Where(c => existingCollectionNames.Contains(c.CollectionName)))
            {
                if (!_storageOptions.MigrationOptions.Backup)
                {
                    database.Database.DropCollection(collection.CollectionName);
                }
                else
                {
                    var backupCollectionName = BackupCollectionName(collection.CollectionName, schema);
                    if (existingCollectionNames.Contains(backupCollectionName))
                    {
						// Assume that the collection is already backed up - just drop the original
						database.Database.DropCollection(collection.CollectionName);
					}
                    else
                    {
						database.Database.RenameCollection(collection.CollectionName, backupCollectionName);
					}
                }
            }
        }


        private void ExecuteMigrate(HangfireDbContext database)
        {
            throw new NotImplementedException($@"The {nameof(MongoMigrationStrategy.Migrate)} is not yet implemented");
        }



        private string BackupCollectionName(string collectionName, MongoSchema schema)
        {
            return $@"{collectionName}.{(int)schema}.{_storageOptions.MigrationOptions.BackupPostfix}";
        }

    }
}
