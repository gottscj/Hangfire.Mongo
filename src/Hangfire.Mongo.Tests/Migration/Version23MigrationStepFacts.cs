using System;
using System.Collections.Generic;
using System.Linq;
using Hangfire.Mongo.Migration;
using Hangfire.Mongo.Migration.Steps.Version23;
using Hangfire.Mongo.Tests.Utils;
using MongoDB.Bson;
using MongoDB.Driver;
using Xunit;

namespace Hangfire.Mongo.Tests.Migration
{
    [Collection("Database")]
    public class Version23MigrationStepFacts
    {
        private readonly IMongoDatabase _database;
        private readonly MongoStorageOptions _storageOptions;

        public Version23MigrationStepFacts(MongoIntegrationTestFixture fixture)
        {
            var dbContext = fixture.CreateDbContext();
            _database = dbContext.Database;
            _storageOptions = new MongoStorageOptions();
        }

        #region Step 00 - CreateStateHistoryCollectionMigrationStep

        [Fact]
        public void ExecuteStep00_CreateStateHistoryCollection_Success()
        {
            // ARRANGE
            var migration = new CreateStateHistoryCollectionMigrationStep();
            var collectionName = _storageOptions.Prefix + ".stateHistory";
            
            // Drop collection if it exists
            _database.DropCollection(collectionName);

            // ACT
            var result = migration.Execute(_database, _storageOptions, new MongoMigrationContext());

            // ASSERT
            Assert.True(result, "Expected migration to be successful, reported 'false'");
            
            var filter = new BsonDocument("name", collectionName);
            var collections = _database.ListCollections(new ListCollectionsOptions { Filter = filter }).ToList();
            Assert.Single(collections);
        }

        [Fact]
        public void ExecuteStep00_CreateStateHistoryCollection_CreatesJobIdIndex()
        {
            // ARRANGE
            var migration = new CreateStateHistoryCollectionMigrationStep();
            var collectionName = _storageOptions.Prefix + ".stateHistory";
            
            // Drop collection if it exists
            _database.DropCollection(collectionName);

            // ACT
            var result = migration.Execute(_database, _storageOptions, new MongoMigrationContext());

            // ASSERT
            Assert.True(result, "Expected migration to be successful, reported 'false'");
            
            var collection = _database.GetCollection<BsonDocument>(collectionName);
            var indexes = collection.Indexes.List().ToList();
            var jobIdIndex = indexes.FirstOrDefault(i => i["name"].AsString == "IX_JobId");
            
            Assert.NotNull(jobIdIndex);
        }

        [Fact]
        public void ExecuteStep00_CreateStateHistoryCollection_DoesNotCreateIfExists()
        {
            // ARRANGE
            var migration = new CreateStateHistoryCollectionMigrationStep();
            var collectionName = _storageOptions.Prefix + ".stateHistory";
            
            // Drop and create the collection manually first
            _database.DropCollection(collectionName);
            _database.CreateCollection(collectionName);

            // ACT
            var result = migration.Execute(_database, _storageOptions, new MongoMigrationContext());

            // ASSERT
            Assert.True(result, "Expected migration to be successful, reported 'false'");
            
            // Verify collection still exists and no duplicate was created
            var filter = new BsonDocument("name", collectionName);
            var collections = _database.ListCollections(new ListCollectionsOptions { Filter = filter }).ToList();
            Assert.Single(collections);
            
            // Index should NOT exist since we created the collection manually (not through migration)
            var collection = _database.GetCollection<BsonDocument>(collectionName);
            var indexes = collection.Indexes.List().ToList();
            var jobIdIndex = indexes.FirstOrDefault(i => i["name"].AsString == "IX_JobId");
            Assert.Null(jobIdIndex);
        }

        #endregion

        #region Step 01 - MigrateStateHistoryToCollectionStep

        [Fact]
        public void ExecuteStep01_MigrateStateHistory_Success()
        {
            // ARRANGE
            var migration = new MigrateStateHistoryToCollectionStep();
            var jobGraphCollection = _database.GetCollection<BsonDocument>(_storageOptions.Prefix + ".jobGraph");
            var stateHistoryCollection = _database.GetCollection<BsonDocument>(_storageOptions.Prefix + ".stateHistory");
            
            jobGraphCollection.DeleteMany("{}");
            stateHistoryCollection.DeleteMany("{}");
            
            var jobs = CreateJobDtosWithStateHistory(jobCount: 3, stateHistoryPerJob: 2);
            jobGraphCollection.InsertMany(jobs);

            // ACT
            var result = migration.Execute(_database, _storageOptions, new MongoMigrationContext());

            // ASSERT
            Assert.True(result, "Expected migration to be successful, reported 'false'");
            
            var migratedHistory = stateHistoryCollection.Find(new BsonDocument()).ToList();
            Assert.Equal(6, migratedHistory.Count); // 3 jobs * 2 states each
            
            foreach (var historyDoc in migratedHistory)
            {
                Assert.True(historyDoc.Contains("JobId"));
                Assert.True(historyDoc.Contains("State"));
                Assert.True(historyDoc["State"].AsBsonDocument.Contains("Name"));
                Assert.True(historyDoc["State"].AsBsonDocument.Contains("CreatedAt"));
            }
        }

        [Fact]
        public void ExecuteStep01_MigrateStateHistory_PreservesJobIdReference()
        {
            // ARRANGE
            var migration = new MigrateStateHistoryToCollectionStep();
            var jobGraphCollection = _database.GetCollection<BsonDocument>(_storageOptions.Prefix + ".jobGraph");
            var stateHistoryCollection = _database.GetCollection<BsonDocument>(_storageOptions.Prefix + ".stateHistory");
            
            jobGraphCollection.DeleteMany("{}");
            stateHistoryCollection.DeleteMany("{}");
            
            var jobs = CreateJobDtosWithStateHistory(jobCount: 2, stateHistoryPerJob: 3);
            jobGraphCollection.InsertMany(jobs);

            // ACT
            var result = migration.Execute(_database, _storageOptions, new MongoMigrationContext());

            // ASSERT
            Assert.True(result, "Expected migration to be successful, reported 'false'");
            
            foreach (var job in jobs)
            {
                var jobId = job["_id"].AsObjectId;
                var historyForJob = stateHistoryCollection.Find(new BsonDocument("JobId", jobId)).ToList();
                Assert.Equal(3, historyForJob.Count);
            }
        }

        [Fact]
        public void ExecuteStep01_MigrateStateHistory_HandlesEmptyStateHistory()
        {
            // ARRANGE
            var migration = new MigrateStateHistoryToCollectionStep();
            var jobGraphCollection = _database.GetCollection<BsonDocument>(_storageOptions.Prefix + ".jobGraph");
            var stateHistoryCollection = _database.GetCollection<BsonDocument>(_storageOptions.Prefix + ".stateHistory");
            
            jobGraphCollection.DeleteMany("{}");
            stateHistoryCollection.DeleteMany("{}");
            
            var jobs = CreateJobDtosWithStateHistory(jobCount: 2, stateHistoryPerJob: 0);
            jobGraphCollection.InsertMany(jobs);

            // ACT
            var result = migration.Execute(_database, _storageOptions, new MongoMigrationContext());

            // ASSERT
            Assert.True(result, "Expected migration to be successful, reported 'false'");
            
            var migratedHistory = stateHistoryCollection.Find(new BsonDocument()).ToList();
            Assert.Empty(migratedHistory);
        }

        [Fact]
        public void ExecuteStep01_MigrateStateHistory_HandlesNoJobs()
        {
            // ARRANGE
            var migration = new MigrateStateHistoryToCollectionStep();
            var jobGraphCollection = _database.GetCollection<BsonDocument>(_storageOptions.Prefix + ".jobGraph");
            var stateHistoryCollection = _database.GetCollection<BsonDocument>(_storageOptions.Prefix + ".stateHistory");
            
            jobGraphCollection.DeleteMany("{}");
            stateHistoryCollection.DeleteMany("{}");

            // ACT
            var result = migration.Execute(_database, _storageOptions, new MongoMigrationContext());

            // ASSERT
            Assert.True(result, "Expected migration to be successful, reported 'false'");
            
            var migratedHistory = stateHistoryCollection.Find(new BsonDocument()).ToList();
            Assert.Empty(migratedHistory);
        }

        #endregion

        #region Step 02 - RemoveStateHistoryFromJobDtoStep

        [Fact]
        public void ExecuteStep02_RemoveStateHistoryFromJobDto_Success()
        {
            // ARRANGE
            var migration = new RemoveStateHistoryFromJobDtoStep();
            var jobGraphCollection = _database.GetCollection<BsonDocument>(_storageOptions.Prefix + ".jobGraph");
            
            jobGraphCollection.DeleteMany("{}");
            
            var jobs = CreateJobDtosWithStateHistory(jobCount: 3, stateHistoryPerJob: 2);
            jobGraphCollection.InsertMany(jobs);

            // ACT
            var result = migration.Execute(_database, _storageOptions, new MongoMigrationContext());

            // ASSERT
            Assert.True(result, "Expected migration to be successful, reported 'false'");
            
            var filter = new BsonDocument("_t", new BsonArray { "BaseJobDto", "ExpiringJobDto", "JobDto" });
            var updatedJobs = jobGraphCollection.Find(filter).ToList();
            
            foreach (var job in updatedJobs)
            {
                Assert.False(job.Contains("StateHistory"), "StateHistory field should have been removed");
            }
        }

        [Fact]
        public void ExecuteStep02_RemoveStateHistoryFromJobDto_PreservesOtherFields()
        {
            // ARRANGE
            var migration = new RemoveStateHistoryFromJobDtoStep();
            var jobGraphCollection = _database.GetCollection<BsonDocument>(_storageOptions.Prefix + ".jobGraph");
            
            jobGraphCollection.DeleteMany("{}");
            
            var jobs = CreateJobDtosWithStateHistory(jobCount: 2, stateHistoryPerJob: 2);
            jobGraphCollection.InsertMany(jobs);

            // ACT
            var result = migration.Execute(_database, _storageOptions, new MongoMigrationContext());

            // ASSERT
            Assert.True(result, "Expected migration to be successful, reported 'false'");
            
            var filter = new BsonDocument("_t", new BsonArray { "BaseJobDto", "ExpiringJobDto", "JobDto" });
            var updatedJobs = jobGraphCollection.Find(filter).ToList();
            
            foreach (var job in updatedJobs)
            {
                Assert.True(job.Contains("_id"));
                Assert.True(job.Contains("StateName"));
                Assert.True(job.Contains("CreatedAt"));
                Assert.True(job.Contains("InvocationData"));
            }
        }

        [Fact]
        public void ExecuteStep02_RemoveStateHistoryFromJobDto_DoesNotAffectOtherDocumentTypes()
        {
            // ARRANGE
            var migration = new RemoveStateHistoryFromJobDtoStep();
            var jobGraphCollection = _database.GetCollection<BsonDocument>(_storageOptions.Prefix + ".jobGraph");
            
            jobGraphCollection.DeleteMany("{}");
            
            // Insert a non-JobDto document with a StateHistory field (shouldn't happen, but let's be safe)
            var otherDoc = new BsonDocument
            {
                ["_id"] = ObjectId.GenerateNewId(),
                ["_t"] = "SomeOtherDto",
                ["StateHistory"] = new BsonArray { new BsonDocument("Name", "Test") }
            };
            jobGraphCollection.InsertOne(otherDoc);

            // ACT
            var result = migration.Execute(_database, _storageOptions, new MongoMigrationContext());

            // ASSERT
            Assert.True(result, "Expected migration to be successful, reported 'false'");
            
            var otherDocs = jobGraphCollection.Find(new BsonDocument("_t", "SomeOtherDto")).ToList();
            Assert.Single(otherDocs);
            Assert.True(otherDocs[0].Contains("StateHistory"), "StateHistory should NOT be removed from non-JobDto documents");
        }

        #endregion

        #region Helper Methods

        private List<BsonDocument> CreateJobDtosWithStateHistory(int jobCount, int stateHistoryPerJob)
        {
            var list = new List<BsonDocument>();

            for (int i = 0; i < jobCount; i++)
            {
                var stateHistory = new BsonArray();
                for (int j = 0; j < stateHistoryPerJob; j++)
                {
                    stateHistory.Add(new BsonDocument
                    {
                        ["Name"] = $"State{j}",
                        ["Reason"] = $"Reason for state {j}",
                        ["CreatedAt"] = DateTime.UtcNow.AddHours(-j),
                        ["Data"] = new BsonDocument
                        {
                            ["Key1"] = "Value1",
                            ["Key2"] = "Value2"
                        }
                    });
                }

                var jobDto = new BsonDocument
                {
                    ["_id"] = ObjectId.GenerateNewId(),
                    ["_t"] = new BsonArray { "BaseJobDto", "ExpiringJobDto", "JobDto" },
                    ["StateName"] = "Enqueued",
                    ["InvocationData"] = "{}",
                    ["Arguments"] = "[]",
                    ["CreatedAt"] = DateTime.UtcNow,
                    ["StateHistory"] = stateHistory,
                    ["Parameters"] = new BsonDocument()
                };
                
                list.Add(jobDto);
            }

            return list;
        }

        #endregion
    }
}