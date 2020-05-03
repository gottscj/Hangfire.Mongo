using System;
using System.Collections.Generic;
using System.Linq;
using MongoDB.Bson;
using MongoDB.Driver;

namespace Hangfire.Mongo.Migration.Strategies.Backup
{
    /// <summary>
    /// No backup strategy
    /// </summary>
    public class CollectionMongoBackupStrategy : MongoBackupStrategy
    {
        /// <summary>
        /// Backs up each hangfire.mongo collection before executing migration
        /// </summary>
        /// <param name="storageOptions"></param>
        /// <param name="database"></param>
        /// <param name="fromSchema"></param>
        /// <param name="toSchema"></param>
        /// <exception cref="InvalidOperationException"></exception>
        public override void Backup(MongoStorageOptions storageOptions, IMongoDatabase database, MongoSchema fromSchema, MongoSchema toSchema)
        {
            var existingCollectionNames = MongoMigrationUtils.ExistingHangfireCollectionNames(database, fromSchema, storageOptions).ToList();
            var backupCollectionNames =
                existingCollectionNames
                    .ToDictionary(k => k, v => 
                        MongoMigrationUtils.GetBackupCollectionName(v, fromSchema, storageOptions));

            // Let's double check that we have not backed up before.
            var existingBackupCollectionName = existingCollectionNames.FirstOrDefault(n => backupCollectionNames.ContainsValue(n));
            if (existingBackupCollectionName != null)
            {
                throw new InvalidOperationException(
                    $"{Environment.NewLine}{existingBackupCollectionName} already exists. Cannot perform backup." +
                    $"{Environment.NewLine}Cannot overwrite existing backups. Please resolve this manually (e.g. by droping collection)." +
                    $"{Environment.NewLine}Please see https://github.com/sergeyzwezdin/Hangfire.Mongo#migration for further information.");
            }

            // Now do the actual backup
            foreach (var collection in backupCollectionNames)
            {
                BackupCollection(database, collection.Key, collection.Value);
            }
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

            var version = MongoVersionHelper.GetVersion(database);
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