using System;
using System.Collections.Generic;
using System.Linq;
using Hangfire.Mongo.Migration;
using Hangfire.Mongo.Migration.Steps.Version24;
using Hangfire.Mongo.Tests.Utils;
using MongoDB.Bson;
using MongoDB.Driver;
using Xunit;

namespace Hangfire.Mongo.Tests.Migration
{
    [Collection("Database")]
    public class Version24MigrationStepFacts
    {
        private readonly IMongoDatabase _database;
        private readonly MongoStorageOptions _storageOptions;

        public Version24MigrationStepFacts(MongoIntegrationTestFixture fixture)
        {
            var dbContext = fixture.CreateDbContext();
            _database = dbContext.Database;
            _storageOptions = new MongoStorageOptions();
        }

        #region Step 00 - AddStateHistoryFieldToJobDtoStep

        [Fact]
        public void ExecuteStep00_AddStateHistoryField_Success()
        {
            // ARRANGE
            var migration = new AddStateHistoryFieldToJobDtoStep();
            var jobGraphCollection = _database.GetCollection<BsonDocument>(_storageOptions.Prefix + ".jobGraph");
            
            jobGraphCollection.DeleteMany("{}");
            
            var jobDto = new BsonDocument
            {
                ["_id"] = ObjectId.GenerateNewId(),
                ["_t"] = new BsonArray { "BaseJobDto", "ExpiringJobDto", "JobDto" },
                ["StateName"] = "Enqueued",
                ["InvocationData"] = "{}",
                ["Arguments"] = "[]",
                ["CreatedAt"] = DateTime.UtcNow,
                ["Parameters"] = new BsonDocument()
            };
            jobGraphCollection.InsertOne(jobDto);

            // ACT
            var result = migration.Execute(_database, _storageOptions, new MongoMigrationContext());

            // ASSERT
            Assert.True(result, "Expected migration to be successful");
            
            var updatedJob = jobGraphCollection.Find(new BsonDocument("_id", jobDto["_id"])).First();
            Assert.True(updatedJob.Contains("StateHistory"));
            Assert.Equal(new BsonArray(), updatedJob["StateHistory"]);
        }

        [Fact]
        public void ExecuteStep00_AddStateHistoryField_IsIdempotent()
        {
            // ARRANGE
            var migration = new AddStateHistoryFieldToJobDtoStep();
            var jobGraphCollection = _database.GetCollection<BsonDocument>(_storageOptions.Prefix + ".jobGraph");
            
            jobGraphCollection.DeleteMany("{}");
            
            var jobDto = new BsonDocument
            {
                ["_id"] = ObjectId.GenerateNewId(),
                ["_t"] = new BsonArray { "BaseJobDto", "ExpiringJobDto", "JobDto" },
                ["StateName"] = "Enqueued",
                ["InvocationData"] = "{}",
                ["Arguments"] = "[]",
                ["CreatedAt"] = DateTime.UtcNow,
                ["Parameters"] = new BsonDocument(),
                ["StateHistory"] = new BsonArray { new BsonDocument { ["Name"] = "Processing" } }
            };
            jobGraphCollection.InsertOne(jobDto);

            // ACT
            var result = migration.Execute(_database, _storageOptions, new MongoMigrationContext());

            // ASSERT
            Assert.True(result, "Expected migration to be successful");
            
            var updatedJob = jobGraphCollection.Find(new BsonDocument("_id", jobDto["_id"])).First();
            Assert.True(updatedJob.Contains("StateHistory"));
            // Should preserve existing StateHistory
            Assert.Single(updatedJob["StateHistory"].AsBsonArray);
        }

        [Fact]
        public void ExecuteStep00_AddStateHistoryField_OnlyUpdatesJobDto()
        {
            // ARRANGE
            var migration = new AddStateHistoryFieldToJobDtoStep();
            var jobGraphCollection = _database.GetCollection<BsonDocument>(_storageOptions.Prefix + ".jobGraph");
            
            jobGraphCollection.DeleteMany("{}");
            
            // Insert a JobDto
            var jobDto = new BsonDocument
            {
                ["_id"] = ObjectId.GenerateNewId(),
                ["_t"] = new BsonArray { "BaseJobDto", "ExpiringJobDto", "JobDto" },
                ["StateName"] = "Enqueued",
                ["InvocationData"] = "{}",
                ["Arguments"] = "[]",
                ["CreatedAt"] = DateTime.UtcNow,
                ["Parameters"] = new BsonDocument()
            };
            jobGraphCollection.InsertOne(jobDto);

            // Insert a non-JobDto document
            var otherDoc = new BsonDocument
            {
                ["_id"] = ObjectId.GenerateNewId(),
                ["_t"] = "SomeOtherDto"
            };
            jobGraphCollection.InsertOne(otherDoc);

            // ACT
            var result = migration.Execute(_database, _storageOptions, new MongoMigrationContext());

            // ASSERT
            Assert.True(result, "Expected migration to be successful");
            
            var updatedJob = jobGraphCollection.Find(new BsonDocument("_id", jobDto["_id"])).First();
            Assert.True(updatedJob.Contains("StateHistory"));
            
            var otherDocAfter = jobGraphCollection.Find(new BsonDocument("_id", otherDoc["_id"])).First();
            Assert.False(otherDocAfter.Contains("StateHistory"));
        }

        #endregion

        #region Step 01 - MigrateStateHistoryBackToJobDtoStep

        [Fact]
        public void ExecuteStep01_MigrateStateHistoryBack_Success()
        {
            // ARRANGE
            var migration = new MigrateStateHistoryBackToJobDtoStep();
            var jobGraphCollection = _database.GetCollection<BsonDocument>(_storageOptions.Prefix + ".jobGraph");
            var stateHistoryCollection = _database.GetCollection<BsonDocument>(_storageOptions.Prefix + ".stateHistory");
            
            jobGraphCollection.DeleteMany("{}");
            stateHistoryCollection.DeleteMany("{}");

            var jobId = ObjectId.GenerateNewId();
            var jobDto = new BsonDocument
            {
                ["_id"] = jobId,
                ["_t"] = new BsonArray { "BaseJobDto", "ExpiringJobDto", "JobDto" },
                ["StateName"] = "Enqueued",
                ["InvocationData"] = "{}",
                ["Arguments"] = "[]",
                ["CreatedAt"] = DateTime.UtcNow,
                ["Parameters"] = new BsonDocument(),
                ["StateHistory"] = new BsonArray()
            };
            jobGraphCollection.InsertOne(jobDto);

            var state1 = new BsonDocument
            {
                ["_id"] = ObjectId.GenerateNewId(),
                ["JobId"] = jobId,
                ["State"] = new BsonDocument
                {
                    ["Name"] = "Enqueued",
                    ["Reason"] = "Initial state",
                    ["CreatedAt"] = DateTime.UtcNow,
                    ["Data"] = new BsonDocument()
                }
            };
            stateHistoryCollection.InsertOne(state1);

            var state2 = new BsonDocument
            {
                ["_id"] = ObjectId.GenerateNewId(),
                ["JobId"] = jobId,
                ["State"] = new BsonDocument
                {
                    ["Name"] = "Processing",
                    ["Reason"] = "Started processing",
                    ["CreatedAt"] = DateTime.UtcNow.AddSeconds(1),
                    ["Data"] = new BsonDocument()
                }
            };
            stateHistoryCollection.InsertOne(state2);

            // ACT
            var result = migration.Execute(_database, _storageOptions, new MongoMigrationContext());

            // ASSERT
            Assert.True(result, "Expected migration to be successful");
            
            var updatedJob = jobGraphCollection.Find(new BsonDocument("_id", jobId)).First();
            Assert.True(updatedJob.Contains("StateHistory"));
            
            var stateHistory = updatedJob["StateHistory"].AsBsonArray;
            Assert.Equal(2, stateHistory.Count);
            Assert.Equal("Enqueued", stateHistory[0].AsBsonDocument["Name"]);
            Assert.Equal("Processing", stateHistory[1].AsBsonDocument["Name"]);
        }

        [Fact]
        public void ExecuteStep01_MigrateStateHistoryBack_MultipleJobs()
        {
            // ARRANGE
            var migration = new MigrateStateHistoryBackToJobDtoStep();
            var jobGraphCollection = _database.GetCollection<BsonDocument>(_storageOptions.Prefix + ".jobGraph");
            var stateHistoryCollection = _database.GetCollection<BsonDocument>(_storageOptions.Prefix + ".stateHistory");
            
            jobGraphCollection.DeleteMany("{}");
            stateHistoryCollection.DeleteMany("{}");

            var jobId1 = ObjectId.GenerateNewId();
            var jobId2 = ObjectId.GenerateNewId();

            jobGraphCollection.InsertMany(new[]
            {
                new BsonDocument
                {
                    ["_id"] = jobId1,
                    ["_t"] = new BsonArray { "BaseJobDto", "ExpiringJobDto", "JobDto" },
                    ["StateName"] = "Enqueued",
                    ["InvocationData"] = "{}",
                    ["Arguments"] = "[]",
                    ["CreatedAt"] = DateTime.UtcNow,
                    ["Parameters"] = new BsonDocument(),
                    ["StateHistory"] = new BsonArray()
                },
                new BsonDocument
                {
                    ["_id"] = jobId2,
                    ["_t"] = new BsonArray { "BaseJobDto", "ExpiringJobDto", "JobDto" },
                    ["StateName"] = "Processing",
                    ["InvocationData"] = "{}",
                    ["Arguments"] = "[]",
                    ["CreatedAt"] = DateTime.UtcNow,
                    ["Parameters"] = new BsonDocument(),
                    ["StateHistory"] = new BsonArray()
                }
            });

            stateHistoryCollection.InsertMany(new[]
            {
                new BsonDocument
                {
                    ["_id"] = ObjectId.GenerateNewId(),
                    ["JobId"] = jobId1,
                    ["State"] = new BsonDocument { ["Name"] = "Enqueued", ["CreatedAt"] = DateTime.UtcNow, ["Data"] = new BsonDocument() }
                },
                new BsonDocument
                {
                    ["_id"] = ObjectId.GenerateNewId(),
                    ["JobId"] = jobId1,
                    ["State"] = new BsonDocument { ["Name"] = "Processing", ["CreatedAt"] = DateTime.UtcNow.AddSeconds(1), ["Data"] = new BsonDocument() }
                },
                new BsonDocument
                {
                    ["_id"] = ObjectId.GenerateNewId(),
                    ["JobId"] = jobId2,
                    ["State"] = new BsonDocument { ["Name"] = "Processing", ["CreatedAt"] = DateTime.UtcNow, ["Data"] = new BsonDocument() }
                }
            });

            // ACT
            var result = migration.Execute(_database, _storageOptions, new MongoMigrationContext());

            // ASSERT
            Assert.True(result, "Expected migration to be successful");
            
            var job1 = jobGraphCollection.Find(new BsonDocument("_id", jobId1)).First();
            Assert.Equal(2, job1["StateHistory"].AsBsonArray.Count);

            var job2 = jobGraphCollection.Find(new BsonDocument("_id", jobId2)).First();
            Assert.Single(job2["StateHistory"].AsBsonArray);
        }

        [Fact]
        public void ExecuteStep01_MigrateStateHistoryBack_NoStateHistory()
        {
            // ARRANGE
            var migration = new MigrateStateHistoryBackToJobDtoStep();
            var jobGraphCollection = _database.GetCollection<BsonDocument>(_storageOptions.Prefix + ".jobGraph");
            var stateHistoryCollection = _database.GetCollection<BsonDocument>(_storageOptions.Prefix + ".stateHistory");
            
            jobGraphCollection.DeleteMany("{}");
            stateHistoryCollection.DeleteMany("{}");

            var jobId = ObjectId.GenerateNewId();
            jobGraphCollection.InsertOne(new BsonDocument
            {
                ["_id"] = jobId,
                ["_t"] = new BsonArray { "BaseJobDto", "ExpiringJobDto", "JobDto" },
                ["StateName"] = "Enqueued",
                ["InvocationData"] = "{}",
                ["Arguments"] = "[]",
                ["CreatedAt"] = DateTime.UtcNow,
                ["Parameters"] = new BsonDocument(),
                ["StateHistory"] = new BsonArray()
            });

            // ACT
            var result = migration.Execute(_database, _storageOptions, new MongoMigrationContext());

            // ASSERT
            Assert.True(result, "Expected migration to be successful");
            
            var job = jobGraphCollection.Find(new BsonDocument("_id", jobId)).First();
            Assert.Empty(job["StateHistory"].AsBsonArray);
        }

        [Fact]
        public void ExecuteStep01_MigrateStateHistoryBack_PreservesExistingStateHistory()
        {
            // ARRANGE
            var migration = new MigrateStateHistoryBackToJobDtoStep();
            var jobGraphCollection = _database.GetCollection<BsonDocument>(_storageOptions.Prefix + ".jobGraph");
            var stateHistoryCollection = _database.GetCollection<BsonDocument>(_storageOptions.Prefix + ".stateHistory");
            
            jobGraphCollection.DeleteMany("{}");
            stateHistoryCollection.DeleteMany("{}");

            var jobId = ObjectId.GenerateNewId();
            var existingState = new BsonDocument
            {
                ["Name"] = "Manual",
                ["Reason"] = "Pre-migration state",
                ["CreatedAt"] = DateTime.UtcNow.AddDays(-1),
                ["Data"] = new BsonDocument()
            };
            
            jobGraphCollection.InsertOne(new BsonDocument
            {
                ["_id"] = jobId,
                ["_t"] = new BsonArray { "BaseJobDto", "ExpiringJobDto", "JobDto" },
                ["StateName"] = "Enqueued",
                ["InvocationData"] = "{}",
                ["Arguments"] = "[]",
                ["CreatedAt"] = DateTime.UtcNow,
                ["Parameters"] = new BsonDocument(),
                ["StateHistory"] = new BsonArray { existingState }
            });

            stateHistoryCollection.InsertOne(new BsonDocument
            {
                ["_id"] = ObjectId.GenerateNewId(),
                ["JobId"] = jobId,
                ["State"] = new BsonDocument { ["Name"] = "Enqueued", ["CreatedAt"] = DateTime.UtcNow, ["Data"] = new BsonDocument() }
            });

            // ACT
            var result = migration.Execute(_database, _storageOptions, new MongoMigrationContext());

            // ASSERT
            Assert.True(result, "Expected migration to be successful");
            
            var job = jobGraphCollection.Find(new BsonDocument("_id", jobId)).First();
            var stateHistory = job["StateHistory"].AsBsonArray;
            Assert.Equal(2, stateHistory.Count);
            // Existing state should still be there
            Assert.Equal("Manual", stateHistory[0].AsBsonDocument["Name"]);
            // New state should be appended
            Assert.Equal("Enqueued", stateHistory[1].AsBsonDocument["Name"]);
        }

        [Fact]
        public void ExecuteStep01_MigrateStateHistoryBack_PreservesStateOrder()
        {
            // ARRANGE
            var migration = new MigrateStateHistoryBackToJobDtoStep();
            var jobGraphCollection = _database.GetCollection<BsonDocument>(_storageOptions.Prefix + ".jobGraph");
            var stateHistoryCollection = _database.GetCollection<BsonDocument>(_storageOptions.Prefix + ".stateHistory");
            
            jobGraphCollection.DeleteMany("{}");
            stateHistoryCollection.DeleteMany("{}");

            var jobId = ObjectId.GenerateNewId();
            var baseTime = DateTime.UtcNow;
            
            jobGraphCollection.InsertOne(new BsonDocument
            {
                ["_id"] = jobId,
                ["_t"] = new BsonArray { "BaseJobDto", "ExpiringJobDto", "JobDto" },
                ["StateName"] = "Completed",
                ["InvocationData"] = "{}",
                ["Arguments"] = "[]",
                ["CreatedAt"] = baseTime,
                ["Parameters"] = new BsonDocument(),
                ["StateHistory"] = new BsonArray()
            });

            // Insert multiple state history entries in chronological order
            var stateProgression = new[]
            {
                ("Enqueued", "Job created", baseTime),
                ("Processing", "Started processing", baseTime.AddSeconds(1)),
                ("Succeeded", "Job completed successfully", baseTime.AddSeconds(2))
            };

            foreach (var (stateName, reason, createdAt) in stateProgression)
            {
                stateHistoryCollection.InsertOne(new BsonDocument
                {
                    ["_id"] = ObjectId.GenerateNewId(),
                    ["JobId"] = jobId,
                    ["State"] = new BsonDocument
                    {
                        ["Name"] = stateName,
                        ["Reason"] = reason,
                        ["CreatedAt"] = createdAt,
                        ["Data"] = new BsonDocument()
                    }
                });
            }

            // ACT
            var result = migration.Execute(_database, _storageOptions, new MongoMigrationContext());

            // ASSERT
            Assert.True(result, "Expected migration to be successful");
            
            var updatedJob = jobGraphCollection.Find(new BsonDocument("_id", jobId)).First();
            var stateHistory = updatedJob["StateHistory"].AsBsonArray;
            
            Assert.Equal(3, stateHistory.Count);
            
            // Verify each state is in the correct order with correct properties
            Assert.Equal("Enqueued", stateHistory[0].AsBsonDocument["Name"]);
            Assert.Equal("Job created", stateHistory[0].AsBsonDocument["Reason"]);
            // Use InRange for DateTime comparison to account for MongoDB precision (milliseconds)
            var createdAt0 = stateHistory[0].AsBsonDocument["CreatedAt"].ToUniversalTime();
            Assert.InRange(createdAt0, baseTime.AddMilliseconds(-1), baseTime.AddMilliseconds(1));
            
            Assert.Equal("Processing", stateHistory[1].AsBsonDocument["Name"]);
            Assert.Equal("Started processing", stateHistory[1].AsBsonDocument["Reason"]);
            var createdAt1 = stateHistory[1].AsBsonDocument["CreatedAt"].ToUniversalTime();
            var expectedTime1 = baseTime.AddSeconds(1);
            Assert.InRange(createdAt1, expectedTime1.AddMilliseconds(-1), expectedTime1.AddMilliseconds(1));
            
            Assert.Equal("Succeeded", stateHistory[2].AsBsonDocument["Name"]);
            Assert.Equal("Job completed successfully", stateHistory[2].AsBsonDocument["Reason"]);
            var createdAt2 = stateHistory[2].AsBsonDocument["CreatedAt"].ToUniversalTime();
            var expectedTime2 = baseTime.AddSeconds(2);
            Assert.InRange(createdAt2, expectedTime2.AddMilliseconds(-1), expectedTime2.AddMilliseconds(1));
        }

        #endregion

        #region Step 02 - DropStateHistoryCollectionStep

        [Fact]
        public void ExecuteStep02_DropStateHistoryCollection_Success()
        {
            // ARRANGE
            var migration = new DropStateHistoryCollectionStep();
            var collectionName = _storageOptions.Prefix + ".stateHistory";
            
            // Create the collection
            _database.CreateCollection(collectionName);
            
            var filter = new BsonDocument("name", collectionName);
            var collections = _database.ListCollections(new ListCollectionsOptions { Filter = filter }).ToList();
            Assert.Single(collections);

            // ACT
            var result = migration.Execute(_database, _storageOptions, new MongoMigrationContext());

            // ASSERT
            Assert.True(result, "Expected migration to be successful");
            
            collections = _database.ListCollections(new ListCollectionsOptions { Filter = filter }).ToList();
            Assert.Empty(collections);
        }

        [Fact]
        public void ExecuteStep02_DropStateHistoryCollection_IsIdempotent()
        {
            // ARRANGE
            var migration = new DropStateHistoryCollectionStep();
            
            // Collection doesn't exist
            var collectionName = _storageOptions.Prefix + ".stateHistory";
            
            // Ensure collection doesn't exist before starting
            try
            {
                _database.DropCollection(collectionName);
            }
            catch
            {
                // Collection doesn't exist, which is fine
            }

            var filter = new BsonDocument("name", collectionName);
            var collections = _database.ListCollections(new ListCollectionsOptions { Filter = filter }).ToList();
            Assert.Empty(collections);

            // ACT - should not fail even if collection doesn't exist
            var result = migration.Execute(_database, _storageOptions, new MongoMigrationContext());

            // ASSERT
            Assert.True(result, "Expected migration to be successful");
            
            collections = _database.ListCollections(new ListCollectionsOptions { Filter = filter }).ToList();
            Assert.Empty(collections);
        }

        [Fact]
        public void ExecuteStep02_DropStateHistoryCollection_DoesNotAffectOtherCollections()
        {
            // ARRANGE
            var migration = new DropStateHistoryCollectionStep();
            
            // Create multiple collections
            var stateHistoryName = _storageOptions.Prefix + ".stateHistory";
            var jobGraphName = _storageOptions.Prefix + ".jobGraph";
            
            _database.CreateCollection(stateHistoryName);
            _database.CreateCollection(jobGraphName);

            // ACT
            var result = migration.Execute(_database, _storageOptions, new MongoMigrationContext());

            // ASSERT
            Assert.True(result, "Expected migration to be successful");
            
            // stateHistory should be gone
            var filter = new BsonDocument("name", stateHistoryName);
            var collections = _database.ListCollections(new ListCollectionsOptions { Filter = filter }).ToList();
            Assert.Empty(collections);
            
            // jobGraph should still exist
            filter = new BsonDocument("name", jobGraphName);
            collections = _database.ListCollections(new ListCollectionsOptions { Filter = filter }).ToList();
            Assert.Single(collections);
        }

        #endregion
    }
}

