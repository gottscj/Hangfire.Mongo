using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Hangfire.Mongo.Database;
using MongoDB.Bson;
using MongoDB.Driver;

namespace Hangfire.Mongo.Migration.Strategies
{
    internal abstract class MongoMigrationStrategyBase : IMongoMigrationStrategy
    {
        protected readonly HangfireDbContext _dbContext;
        protected readonly MongoStorageOptions _storageOptions;

        public MongoMigrationStrategyBase(HangfireDbContext dbContext, MongoStorageOptions storageOptions)
        {
            _dbContext = dbContext;
            _storageOptions = storageOptions;
        }


        public virtual void Execute(MongoSchema fromSchema, MongoSchema toSchema)
        {
            // First we backup...
            Backup(fromSchema, toSchema);
            // ...then we migrate
            Migrate(fromSchema, toSchema);
        }


        protected virtual void Backup(MongoSchema fromSchema, MongoSchema toSchema)
        {
            switch (_storageOptions.MigrationOptions.BackupStrategy)
            {
                case MongoBackupStrategy.None:
                    BackupStrategyNone(_dbContext.Database, fromSchema, toSchema);
                    break;

                case MongoBackupStrategy.Collections:
                    BackupStrategyCollection(_dbContext.Database, fromSchema, toSchema);
                    break;

                case MongoBackupStrategy.Database:
                    BackupStrategyDatabase(_dbContext.Client, _dbContext.Database, fromSchema, toSchema);
                    break;

                default:
                    throw new ArgumentOutOfRangeException($@"Unknown backup strategy: {_storageOptions.MigrationOptions.BackupStrategy}", $@"{nameof(MongoMigrationOptions)}.{nameof(MongoMigrationOptions.BackupStrategy)}");
            }
        }


        protected virtual void Migrate(MongoSchema fromSchema, MongoSchema toSchema)
        {
            var migrationRunner = new MongoMigrationRunner(_dbContext, _storageOptions);
            migrationRunner.Execute(fromSchema, toSchema);
        }


        protected virtual void BackupStrategyNone(IMongoDatabase database, MongoSchema fromSchema, MongoSchema toSchema)
        {
        }


        protected virtual void BackupStrategyCollection(IMongoDatabase database, MongoSchema fromSchema, MongoSchema toSchema)
        {
            var existingCollectionNames = ExistingHangfireCollectionNames(fromSchema).ToList();
            var backupCollectionNames =
                existingCollectionNames.ToDictionary(k => k, v => GetBackupCollectionName(v, fromSchema));

            // Let's double check that we have not backed up before.
            var existingBackupcollectionName = existingCollectionNames.FirstOrDefault(n => backupCollectionNames.ContainsValue(n));
            if (existingBackupcollectionName != null)
            {
                throw new InvalidOperationException(
                    $"{Environment.NewLine}{existingBackupcollectionName} already exists. Cannot perform backup." +
                    $"{Environment.NewLine}Cannot overwrite existing backups. Please resolve this manually (e.g. by droping collection)." +
                    $"{Environment.NewLine}Please see https://github.com/sergeyzwezdin/Hangfire.Mongo#migration for further information.");
            }

            // Now do the actual backup
            foreach (var collection in backupCollectionNames)
            {
                BackupCollection(database, collection.Key, collection.Value);
            }
        }


        protected virtual void BackupStrategyDatabase(IMongoClient client, IMongoDatabase database, MongoSchema fromSchema, MongoSchema toSchema)
        {
            var databaseName = database.DatabaseNamespace.DatabaseName;
            var backupDatabaseName = GetBackupDatabaseName(databaseName, fromSchema);
            var db = client.GetDatabase(DatabaseNamespace.Admin.DatabaseName);
            var doc = new BsonDocument(new Dictionary<string, object>
            {
                { "copydb", 1 },
                //{ "fromhost", "localhost" },
                { "fromdb", databaseName },
                { "todb", backupDatabaseName }
            });
            db.RunCommand(new BsonDocumentCommand<BsonDocument>(doc));
        }


        /// <summary>
        /// Find hangfire collection namespaces by reflecting over properties on database.
        /// </summary>
        protected IEnumerable<string> ExistingHangfireCollectionNames(MongoSchema schema)
        {
            var existingCollectionNames = ExistingDatabaseCollectionNames().ToList();
            return schema.CollectionNames(_storageOptions.Prefix).Where(c => existingCollectionNames.Contains(c));
        }


        /// <summary>
        /// Gets the existing collection names from database
        /// </summary>
        protected IEnumerable<string> ExistingDatabaseCollectionNames()
        {
            return _dbContext.Database.ListCollections().ToList().Select(c => c["name"].AsString);
        }


        /// <summary>
        /// Generate the name of tha tbackup collection based on the original collection name and schema.
        /// </summary>
        protected string GetBackupDatabaseName(string databaseName, MongoSchema schema)
        {
            return $@"{databaseName}-{(int)schema}-{_storageOptions.MigrationOptions.BackupPostfix}";
        }


        /// <summary>
        /// Generate the name of tha tbackup collection based on the original collection name and schema.
        /// </summary>
        protected string GetBackupCollectionName(string collectionName, MongoSchema schema)
        {
            return $@"{collectionName}.{(int)schema}.{_storageOptions.MigrationOptions.BackupPostfix}";
        }


        /// <summary>
        /// Backups the collection in database identified by collectionName.
        /// </summary>
        /// <param name="database">Referance to the mongo database.</param>
        /// <param name="collectionName">The name of the collection to backup.</param>
        /// <param name="backupCollectionName">Tha name of the backup collection.</param>
        protected virtual void BackupCollection(IMongoDatabase database, string collectionName, string backupCollectionName)
        {
            var aggregate = new BsonDocument(new Dictionary<string, object>
            {
                {
                    "aggregate", collectionName
                },
                {
                    "pipeline", new []
                    {
                        new Dictionary<string, object> { { "$match", new BsonDocument() } },
                        new Dictionary<string, object> { { "$out", backupCollectionName } }
                    }
                },
                {
                    "allowDiskUse", true
                },
                {
                    // As of MongoDB 3.4 cursor is no longer
                    //  optional, but can be set to "empty".
                    // https://docs.mongodb.com/manual/reference/command/aggregate/
                    "cursor", new BsonDocument()
                }
            });

            var serverStatus = database.RunCommand<BsonDocument>(new BsonDocument("buildinfo", 1));
            if (serverStatus.Contains("version"))
            {
                var version = Version.Parse(serverStatus["version"].AsString);
                if (version < new Version(2, 6))
                {
                    throw new InvalidOperationException("Hangfire.Mongo is not able to backup collections in MongoDB running a version prior to 2.6");
                }
                if (version >= new Version(3, 2))
                {
                    // The 'bypassDocumentValidation' was introduced in version 3.2
                    // https://docs.mongodb.com/manual/release-notes/3.2/#rel-notes-document-validation
                    aggregate["bypassDocumentValidation"] = true;
                }
            }

            var dbSource = database.GetCollection<BsonDocument>(collectionName);
            var indexes = dbSource.Indexes.List().ToList().Where(idx => idx["name"] != "_id_").ToList();
            if (indexes.Any())
            {
                var dbBackup = database.GetCollection<BsonDocument>(backupCollectionName);
                foreach (var index in indexes)
                {
                    var newIndex = new BsonDocumentIndexKeysDefinition<BsonDocument>(index["key"].AsBsonDocument);
                    var newIndexKeys = index.Names.ToList();

                    var newOptions = new CreateIndexOptions();


                    foreach (var key in newIndexKeys)
                    {
                        switch (key)
                        {
                            case "v": newOptions.Version = index[key].AsInt32; break;
                            case "name": newOptions.Name = index[key].AsString; break;
                            case "unique": newOptions.Unique = index[key].AsBoolean; break;
                            case "sparse": newOptions.Sparse = index[key].AsBoolean; break;
                            case "expireAfterSeconds": newOptions.ExpireAfter = TimeSpan.FromSeconds(index[key].AsInt64); break;
                            case "background": newOptions.Background = index[key].AsBoolean; break;
                            case "textIndexVersion": newOptions.TextIndexVersion = index[key].AsInt32; break;
                            case "default_language": newOptions.DefaultLanguage = index[key].AsString; break;
                            case "language_override": newOptions.LanguageOverride = index[key].AsString; break;
                            case "weights": newOptions.Weights = index[key].AsBsonDocument; break;
                            case "min": newOptions.Min = index[key].AsDouble; break;
                            case "max": newOptions.Max = index[key].AsDouble; break;
                            case "bits": newOptions.Bits = index[key].AsInt32; break;
                            case "2dsphereIndexVersion": newOptions.SphereIndexVersion = index[key].AsInt32; break;
                            case "bucketSize": newOptions.BucketSize = index[key].AsDouble; break;
                        }
                    }

                    dbBackup.Indexes.CreateOne(newIndex, newOptions);
                }
            }

            database.RunCommand(new BsonDocumentCommand<BsonDocument>(aggregate));
        }
    }
}
