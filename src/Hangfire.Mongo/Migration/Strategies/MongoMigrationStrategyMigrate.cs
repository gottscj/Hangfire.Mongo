using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Hangfire.Mongo.Database;
using MongoDB.Bson;
using MongoDB.Driver;

namespace Hangfire.Mongo.Migration.Strategies
{
    /// <summary>
    /// Implements the "Migrate" strategy.
    /// Migrate the hangfire collections from current schema to required
    /// </summary>
    internal class MongoMigrationStrategyMigrate : MongoMigrationStrategyBase
    {

        public MongoMigrationStrategyMigrate(HangfireDbContext dbContext, MongoStorageOptions storageOptions)
            : base(dbContext, storageOptions)
        {
        }

        public override void Migrate(MongoSchema fromSchema, MongoSchema toSchema)
        {
            if (_storageOptions.MigrationOptions.Backup)
            {
                foreach (var collectionName in ExistingHangfireCollectionNameSpaces(fromSchema))
                {
                    BackupCollection(_dbContext, collectionName, toSchema);
                }
            }

            var migrationRunner = new MongoMigrationRunner(_dbContext, _storageOptions);
            migrationRunner.Execute(fromSchema, toSchema);
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
                    var newIndex = new BsonDocumentIndexKeysDefinition<BsonDocument>(index["key"].AsBsonDocument);

                    var keyTranslation = new List<dynamic>
                    {
                        new
                        {
                            Key = "v",
                            Name = nameof(CreateIndexOptions.Version),
                            Convert = (Func<BsonValue, int>)(i => i.AsInt32)
                        },
                        new
                        {
                            Key = "name",
                            Name = nameof(CreateIndexOptions.Name),
                            Convert = (Func<BsonValue, string>)(i => i.AsString)
                        },
                        new
                        {
                            Key = "unique",
                            Name = nameof(CreateIndexOptions.Unique),
                            Convert = (Func<BsonValue, bool>)(i => i.AsBoolean)
                        },
                        new
                        {
                            Key = "sparse",
                            Name = nameof(CreateIndexOptions.Sparse),
                            Convert = (Func<BsonValue, bool>)(i => i.AsBoolean)
                        },
                        new
                        {
                            Key = "expireAfterSeconds",
                            Name = nameof(CreateIndexOptions.ExpireAfter),
                            Convert = (Func<BsonValue, TimeSpan>)(i => TimeSpan.FromSeconds(i.AsInt64))
                        },
                        new
                        {
                            Key = "background",
                            Name = nameof(CreateIndexOptions.Background),
                            Convert = (Func<BsonValue, bool>)(i => i.AsBoolean)
                        },
                        new
                        {
                            Key = "textIndexVersion",
                            Name = nameof(CreateIndexOptions.TextIndexVersion),
                            Convert = (Func<BsonValue, int>)(i => i.AsInt32)
                        },
                        new
                        {
                            Key = "default_language",
                            Name = nameof(CreateIndexOptions.DefaultLanguage),
                            Convert = (Func<BsonValue, string>)(i => i.AsString)
                        },
                        new
                        {
                            Key = "language_override",
                            Name = nameof(CreateIndexOptions.LanguageOverride),
                            Convert = (Func<BsonValue, string>)(i => i.AsString)
                        },
                        new
                        {
                            Key = "weights",
                            Name = nameof(CreateIndexOptions.Weights),
                            Convert = (Func<BsonValue, BsonDocument>)(i => i.AsBsonDocument)
                        },
                        new
                        {
                            Key = "min",
                            Name = nameof(CreateIndexOptions.Min),
                            Convert = (Func<BsonValue, double>)(i => i.AsDouble)
                        },
                        new
                        {
                            Key = "max",
                            Name = nameof(CreateIndexOptions.Max),
                            Convert = (Func<BsonValue, double>)(i => i.AsDouble)
                        },
                        new
                        {
                            Key = "bits",
                            Name = nameof(CreateIndexOptions.Bits),
                            Convert = (Func<BsonValue, int>)(i => i.AsInt32)
                        },
                        new
                        {
                            Key = "2dsphereIndexVersion",
                            Name = nameof(CreateIndexOptions.SphereIndexVersion),
                            Convert = (Func<BsonValue, int>)(i => i.AsInt32)
                        },
                        new
                        {
                            Key = "bucketSize",
                            Name = nameof(CreateIndexOptions.BucketSize),
                            Convert = (Func<BsonValue, double>)(i => i.AsDouble)
                        },
                        new
                        {
                            Key = "partialFilterExpression",
                            Name = "Unsupported",
                        },
                        new
                        {
                            Key = "collation",
                            Name = "Unsupported",
                        },
                    };

                    var newOptions = new CreateIndexOptions();
                    foreach (var element in index.Where(e => keyTranslation.Any(t => e.Name == t.Key)))
                    {
                        var translation = keyTranslation.First(t => element.Name == t.Key);
                        var prop = newOptions.GetType().GetTypeInfo().GetProperty(translation.Name);
                        if (prop != null)
                        {
                            prop.SetValue(newOptions, translation.Convert(element.Value));
                        }
                    }
                    dbBackup.Indexes.CreateOne(newIndex, newOptions);
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



    }
}
