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
using MongoDB.Driver;
using Xunit;

namespace Hangfire.Mongo.Tests.Migration.Mongo
{
    [Collection("Database")]
    public class MigrationFacts
    {
        [Theory]
        [InlineData(null)]
        [InlineData("Hangfire-Mongo-Schema-004.zip")]
        [InlineData("Hangfire-Mongo-Schema-005.zip")]
        [InlineData("Hangfire-Mongo-Schema-006.zip")]
        [InlineData("Hangfire-Mongo-Schema-007.zip")]
        [InlineData("Hangfire-Mongo-Schema-008.zip")]
        [InlineData("Hangfire-Mongo-Schema-009.zip")]
        [InlineData("Hangfire-Mongo-Schema-010.zip")]
        [InlineData("Hangfire-Mongo-Schema-011.zip")]
        [InlineData("Hangfire-Mongo-Schema-012.zip")]
        [InlineData("Hangfire-Mongo-Schema-013.zip")]
        public void Migrate_Full_Success(string seedFile)
        {
            using (var dbContext = new HangfireDbContext(ConnectionUtils.GetConnectionString(), "Hangfire-Mongo-Migration-Tests"))
            {
                // ARRANGE
                dbContext.Client.DropDatabase(dbContext.Database.DatabaseNamespace.DatabaseName);
                if (seedFile != null)
                {
                    SeedCollectionFromZipArchive(dbContext, Path.Combine("Migration", seedFile));
                }

                var storageOptions = new MongoStorageOptions
                {
                    MigrationOptions = new MongoMigrationOptions
                    {
                        Strategy = MongoMigrationStrategy.Migrate,
                        BackupStrategy = MongoBackupStrategy.None
                    }
                };

                var migrationManager = new MongoMigrationManager(storageOptions, dbContext);

                // ACT
                migrationManager.Migrate();

                // ASSERT
                AssertDataIntegrity(dbContext);
            }
        }
        
        [Fact]
        public void Migrate_DropNoBackup_Success()
        {
            using (var dbContext = new HangfireDbContext(ConnectionUtils.GetConnectionString(), "Hangfire-Mongo-Migration-Tests"))
            {
                // ARRANGE
                dbContext.Client.DropDatabase(dbContext.Database.DatabaseNamespace.DatabaseName);
                SeedCollectionFromZipArchive(dbContext, Path.Combine("Migration", "Hangfire-Mongo-Schema-006.zip"));

                var storageOptions = new MongoStorageOptions
                {
                    MigrationOptions = new MongoMigrationOptions
                    {
                        Strategy = MongoMigrationStrategy.Drop,
                        BackupStrategy = MongoBackupStrategy.None
                    }
                };

                var migrationManager = new MongoMigrationManager(storageOptions, dbContext);

                // ACT
                migrationManager.Migrate();

                // ASSERT
                AssertDataIntegrity(dbContext);
            }
        }

        private static void AssertDataIntegrity(HangfireDbContext dbContext)
        {
            var jobGraphDtos = dbContext.JobGraph.Find(new BsonDocument()).ToList();
            var locks = dbContext.DistributedLock.Find(new BsonDocument()).ToList();
            var schema = dbContext.Schema.Find(new BsonDocument()).ToList();
            var servers = dbContext.Server.Find(new BsonDocument()).ToList();
        }
        
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