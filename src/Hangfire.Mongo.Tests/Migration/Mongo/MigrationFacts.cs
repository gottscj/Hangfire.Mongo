using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using Hangfire.Mongo.Database;
using Hangfire.Mongo.Migration;
using Hangfire.Mongo.Tests.Utils;
using MongoDB.Bson;
using MongoDB.Bson.IO;
using MongoDB.Bson.Serialization;
using Xunit;

namespace Hangfire.Mongo.Tests.Migration.Mongo
{
    [Collection("Database")]
    public class MigrationFacts
    {

        #region Migration Unit Tests

        [Fact]
        public void FullMigration_NewDatabase_MigrationComplete()
        {
            FullMigration(null);
        }


        [Fact]
        public void FullMigration_FromSchema004_MigrationComplete()
        {
            FullMigration("Hangfire-Mongo-Schema-004.zip");
        }


        [Fact]
        public void FullMigration_FromSchema005_MigrationComplete()
        {
            FullMigration("Hangfire-Mongo-Schema-005.zip");
        }


        [Fact]
        public void FullMigration_FromSchema006_MigrationComplete()
        {
            FullMigration("Hangfire-Mongo-Schema-006.zip");
        }


        [Fact]
        public void FullMigration_FromSchema007_MigrationComplete()
        {
            FullMigration("Hangfire-Mongo-Schema-007.zip");
        }


        [Fact]
        public void FullMigration_FromSchema008_MigrationComplete()
        {
            FullMigration("Hangfire-Mongo-Schema-008.zip");
        }


        [Fact]
        public void FullMigration_FromSchema009_MigrationComplete()
        {
            FullMigration("Hangfire-Mongo-Schema-009.zip");
        }


        [Fact]
        public void FullMigration_FromSchema010_MigrationComplete()
        {
            FullMigration("Hangfire-Mongo-Schema-010.zip");
        }


        [Fact]
        public void FullMigration_FromSchema011_MigrationComplete()
        {
            FullMigration("Hangfire-Mongo-Schema-011.zip");
        }


        [Fact]
        public void FullMigration_FromSchema012_MigrationComplete()
        {
            FullMigration("Hangfire-Mongo-Schema-012.zip");
        }
        
        
        [Fact]
        public void FullMigration_FromSchema013_MigrationComplete()
        {
            FullMigration("Hangfire-Mongo-Schema-013.zip");
        }
        

        #endregion


        #region Test Template

        private void FullMigration(string seedFile)
        {
            using (var connection = new HangfireDbContext(ConnectionUtils.GetConnectionString(), "Hangfire-Mongo-Migration-Tests"))
            {
                // ARRANGE
                connection.Client.DropDatabase(connection.Database.DatabaseNamespace.DatabaseName);
                if (seedFile != null)
                {
                    SeedCollectionFromZipArchive(connection, Path.Combine("Migration", seedFile));
                }

                var storageOptions = new MongoStorageOptions
                {
                    MigrationOptions = new MongoMigrationOptions
                    {
                        Strategy = MongoMigrationStrategy.Migrate,
                        BackupStrategy = MongoBackupStrategy.None
                    }
                };

                var migrationManager = new MongoMigrationManager(storageOptions);

                // ACT
                migrationManager.Migrate(connection);

                // ASSERT
            }
        }

        #endregion


        #region Private Helper Methods

        private static void SeedCollectionFromZipArchive(HangfireDbContext connection, string fileName)
        {
            using (var zip = new ZipArchive(File.OpenRead(fileName)))
            {
                foreach (var entry in zip.Entries)
                {
                    using (var s = new StreamReader(entry.Open()))
                    {
                        SeedCollectionFromJson(connection, Path.GetFileNameWithoutExtension(entry.Name), s);
                    }
                }
            }
        }


        private static void SeedCollectionFromJson(HangfireDbContext connection, string collectionName, TextReader json)
        {
            var documents = new List<BsonDocument>();
            using (var jsonReader = new JsonReader(json))
            {
                while (!jsonReader.IsAtEndOfFile())
                {
                    var value = BsonSerializer.Deserialize<BsonValue>(jsonReader);
                    if (value.BsonType == BsonType.Document)
                    {
                        documents.Add(value.AsBsonDocument);
                    }
                    else if (value.BsonType == BsonType.Array)
                    {
                        documents.AddRange(value.AsBsonArray.Values.ToList().Select(v => v.AsBsonDocument));
                    }
                }
            }

            if (!documents.Any())
            {
                throw new InvalidOperationException($@"The JSON does not contain any documents to import into {collectionName}");
            }

            connection.Database.GetCollection<BsonDocument>(collectionName).InsertMany(documents);
        }

        #endregion
    }
}