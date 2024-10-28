using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Hangfire.Mongo.Database;
using Hangfire.Mongo.Migration;
using Hangfire.Mongo.Migration.Strategies;
using Hangfire.Mongo.Migration.Strategies.Backup;
using Hangfire.Mongo.Tests.Utils;
using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Driver;
using Xunit;

namespace Hangfire.Mongo.Tests.Migration.Mongo
{
    [Collection("Database")]
    public class MigrationFacts
    {
        private readonly MongoIntegrationTestFixture _fixture;

        public MigrationFacts(MongoIntegrationTestFixture fixture)
        {
            fixture.CleanDatabase();
            _fixture = fixture;
        }

        [Theory]
        [InlineData(null, false)]
        [InlineData("Hangfire-Mongo-Schema-004.zip", false)]
        [InlineData("Hangfire-Mongo-Schema-005.zip", true)]
        [InlineData("Hangfire-Mongo-Schema-006.zip", true)]
        [InlineData("Hangfire-Mongo-Schema-007.zip", true)]
        [InlineData("Hangfire-Mongo-Schema-008.zip", true)]
        [InlineData("Hangfire-Mongo-Schema-009.zip", true)]
        [InlineData("Hangfire-Mongo-Schema-010.zip", true)]
        [InlineData("Hangfire-Mongo-Schema-011.zip", true)]
        [InlineData("Hangfire-Mongo-Schema-012.zip", true)]
        [InlineData("Hangfire-Mongo-Schema-013.zip", true)]
        [InlineData("Hangfire-Mongo-Schema-014.zip", true)]
        [InlineData("Hangfire-Mongo-Schema-015.zip", true)]
        [InlineData("Hangfire-Mongo-Schema-016.zip", true)]
        [InlineData("Hangfire-Mongo-Schema-017.zip", true)]
        //[InlineData("Hangfire-Mongo-Schema-018.zip", true)]
        //[InlineData("Hangfire-Mongo-Schema-019.zip", true)]
        public void Migrate_Full_Success(string seedFile, bool assertCollectionHasItems)
        {
            var dbContext = _fixture.CreateDbContext("Hangfire-Mongo-Migration-Tests");

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
                    MigrationStrategy = new MigrateMongoMigrationStrategy(),
                    BackupStrategy = new NoneMongoBackupStrategy()
                }
            };

            var migrationManager = new MongoMigrationManager(storageOptions, dbContext.Database);

            // ACT
            migrationManager.MigrateUp();

            // ASSERT
            AssertDataIntegrity(dbContext, assertCollectionHasItems);
        }

        [Fact]
        public async Task Migrate_MultipleInstances_ThereCanBeOnlyOne()
        {
            // ARRANGE
            var dbContext = _fixture.CreateDbContext();
            await dbContext.Database.DropCollectionAsync(dbContext.Schema.CollectionNamespace.CollectionName);
            var storageOptions = new MongoStorageOptions
            {
                MigrationOptions = new MongoMigrationOptions
                {
                    MigrationStrategy = new DropMongoMigrationStrategy(),
                    BackupStrategy = new NoneMongoBackupStrategy()
                }
            };

            var signal = new ManualResetEvent(false);
            var signalToStart = new ManualResetEvent(false);
            var taskCount = 10;
            var tasks = new Task<bool>[taskCount];
            var count = 0;

            // ACT
            for (var i = 0; i < taskCount; i++)
            {
                tasks[i] = Task.Factory.StartNew(() =>
                {
                    count++;
                    if (count == taskCount)
                    {
                        signalToStart.Set();
                    }

                    signal.WaitOne();
                    using var lockHandle = new MigrationLock(dbContext.Database, storageOptions.Prefix,
                        storageOptions.MigrationLockTimeout);
                    lockHandle.AcquireLock();
                    
                    var mgr = new MongoMigrationManager(storageOptions, dbContext.Database);
                    return mgr.MigrateUp();
                }, TaskCreationOptions.LongRunning);
            }

            signalToStart.WaitOne();
            signal.Set();
            var results = await Task.WhenAll(tasks).ContinueWith(t => t.Result);

            // ASSERT
            Assert.True(results.Single(b => b));
        }

        [Fact]
        public void Migrate_DropNoBackup_Success()
        {
            var dbContext = _fixture.CreateDbContext("Hangfire-Mongo-Migration-Tests");

            // ARRANGE
            dbContext.Client.DropDatabase(dbContext.Database.DatabaseNamespace.DatabaseName);
            SeedCollectionFromZipArchive(dbContext, Path.Combine("Migration", "Hangfire-Mongo-Schema-006.zip"));

            var storageOptions = new MongoStorageOptions
            {
                MigrationOptions = new MongoMigrationOptions
                {
                    MigrationStrategy = new DropMongoMigrationStrategy(),
                    BackupStrategy = new NoneMongoBackupStrategy()
                }
            };
            var migrationManager = new MongoMigrationManager(storageOptions, dbContext.Database);

            // ACT
            migrationManager.MigrateUp();

            // ASSERT
            AssertDataIntegrity(dbContext, assertCollectionHasItems: false);
        }

        [Fact]
        public void Migrate_MigrateDownUsingDrop_Success()
        {
            // ARRANGE
            var dbContext = _fixture.CreateDbContext("Hangfire-Mongo-Migration-Tests");

            dbContext.Client.DropDatabase(dbContext.Database.DatabaseNamespace.DatabaseName);
            SeedCollectionFromZipArchive(dbContext, Path.Combine("Migration", "Hangfire-Mongo-Schema-006.zip"));

            var storageOptions = new MongoStorageOptions
            {
                MigrationOptions = new MongoMigrationOptions
                {
                    MigrationStrategy = new DropMongoMigrationStrategy(),
                    BackupStrategy = new NoneMongoBackupStrategy()
                }
            };
            var migrationManager = new MongoMigrationManager(storageOptions, dbContext.Database);
            migrationManager.MigrateUp();

            // ACT
            var testMgr = new TestMongoMigrationManager(new MongoStorageOptions
            {
                MigrationOptions = new MongoMigrationOptions
                {
                    MigrationStrategy = new DropMongoMigrationStrategy(),
                    BackupStrategy = new NoneMongoBackupStrategy()
                }
            }, dbContext.Database);
            testMgr.MigrateUp();

            // ASSERT
            AssertDataIntegrity(dbContext, assertCollectionHasItems: false);
        }

        
        [Fact]
        public void Migrate_MigrateDown_Fails()
        {
            // ARRANGE
            var dbContext = _fixture.CreateDbContext("Hangfire-Mongo-Migration-Tests");

            dbContext.Client.DropDatabase(dbContext.Database.DatabaseNamespace.DatabaseName);
            SeedCollectionFromZipArchive(dbContext, Path.Combine("Migration", "Hangfire-Mongo-Schema-006.zip"));

            var storageOptions = new MongoStorageOptions
            {
                MigrationOptions = new MongoMigrationOptions
                {
                    MigrationStrategy = new DropMongoMigrationStrategy(),
                    BackupStrategy = new NoneMongoBackupStrategy()
                }
            };
            var migrationManager = new MongoMigrationManager(storageOptions, dbContext.Database);
            migrationManager.MigrateUp();

            // ACT
            var testMgr = new TestMongoMigrationManager(new MongoStorageOptions
            {
                MigrationOptions = new MongoMigrationOptions
                {
                    MigrationStrategy = new MigrateMongoMigrationStrategy(),
                    BackupStrategy = new NoneMongoBackupStrategy()
                }
            }, dbContext.Database);
            var exception = Assert.Throws<InvalidOperationException>(() => testMgr.MigrateUp());

            // ASSERT
            Assert.NotNull(exception);
        }

        private static void AssertDataIntegrity(HangfireDbContext dbContext, bool assertCollectionHasItems)
        {
            var jobGraphDtos = dbContext.JobGraph.Find(new BsonDocument()).ToList();
            var locks = dbContext.DistributedLock.Find(new BsonDocument()).ToList();
            var schema = dbContext.Schema.Find(new BsonDocument()).ToList();
            var servers = dbContext.Server.Find(new BsonDocument()).ToList();


            if (assertCollectionHasItems)
            {
                AssertCollectionNotEmpty(jobGraphDtos, nameof(dbContext.JobGraph));
                AssertCollectionNotEmpty(locks, nameof(dbContext.DistributedLock));
                AssertCollectionNotEmpty(schema, nameof(dbContext.Schema));
                AssertCollectionNotEmpty(servers, nameof(dbContext.Server));
            }
        }

        private static void AssertCollectionNotEmpty(IEnumerable<BsonDocument> collection, string collectionName)
        {
            Assert.True(collection.Any(), $"Expected '{collectionName}' to have items");
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
            if (collectionName.Equals("hangfire.migrationLock"))
            {
                return;
            }

            var documents = new List<BsonDocument>();
            using (var jsonReader = new MongoDB.Bson.IO.JsonReader(json))
            {
                while (!jsonReader.IsAtEndOfFile())
                {
                    try
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
                    catch (Exception e)
                    {
                        Console.WriteLine($"Error seeding collection {collectionName}: {e}");

                        throw;
                    }
                }
            }

            if (!documents.Any())
            {
                throw new InvalidOperationException(
                    $@"The JSON does not contain any documents to import into {collectionName}");
            }

            connection.Database.GetCollection<BsonDocument>(collectionName).InsertMany(documents);
        }

        #endregion
    }
}