using System;
using Hangfire.Mongo.Migration;
using Hangfire.Mongo.Migration.Steps.Version25;
using Hangfire.Mongo.Tests.Utils;
using MongoDB.Bson;
using MongoDB.Driver;
using Xunit;

namespace Hangfire.Mongo.Tests.Migration
{
    [Collection("Database")]
    public class Version25MigrationStepFacts
    {
        private readonly IMongoDatabase _database;
        private readonly MongoStorageOptions _storageOptions;

        public Version25MigrationStepFacts(MongoIntegrationTestFixture fixture)
        {
            var dbContext = fixture.CreateDbContext();
            _database = dbContext.Database;
            _storageOptions = new MongoStorageOptions();
        }

        #region Step 00 - AddFetchTokenFieldToJobDtoStep

        [Fact]
        public void ExecuteStep00_AddFetchTokenField_Success()
        {
            // ARRANGE
            var migration = new AddFetchTokenFieldToJobDtoStep();
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
                ["StateHistory"] = new BsonArray()
            };
            jobGraphCollection.InsertOne(jobDto);

            // ACT
            var result = migration.Execute(_database, _storageOptions, new MongoMigrationContext());

            // ASSERT
            Assert.True(result, "Expected migration to be successful");

            var updatedJob = jobGraphCollection.Find(new BsonDocument("_id", jobDto["_id"])).First();
            Assert.True(updatedJob.Contains("FetchToken"));
            Assert.Equal(BsonNull.Value, updatedJob["FetchToken"]);
        }

        [Fact]
        public void ExecuteStep00_AddFetchTokenField_IsIdempotent()
        {
            // ARRANGE
            var migration = new AddFetchTokenFieldToJobDtoStep();
            var jobGraphCollection = _database.GetCollection<BsonDocument>(_storageOptions.Prefix + ".jobGraph");

            jobGraphCollection.DeleteMany("{}");

            var existingToken = Guid.NewGuid().ToString("N");
            var jobDto = new BsonDocument
            {
                ["_id"] = ObjectId.GenerateNewId(),
                ["_t"] = new BsonArray { "BaseJobDto", "ExpiringJobDto", "JobDto" },
                ["StateName"] = "Processing",
                ["InvocationData"] = "{}",
                ["Arguments"] = "[]",
                ["CreatedAt"] = DateTime.UtcNow,
                ["Parameters"] = new BsonDocument(),
                ["StateHistory"] = new BsonArray(),
                ["FetchToken"] = existingToken
            };
            jobGraphCollection.InsertOne(jobDto);

            // ACT
            var result = migration.Execute(_database, _storageOptions, new MongoMigrationContext());

            // ASSERT
            Assert.True(result, "Expected migration to be successful");

            // Should preserve existing FetchToken value.
            var updatedJob = jobGraphCollection.Find(new BsonDocument("_id", jobDto["_id"])).First();
            Assert.Equal(existingToken, updatedJob["FetchToken"].AsString);
        }

        [Fact]
        public void ExecuteStep00_AddFetchTokenField_OnlyUpdatesJobDto()
        {
            // ARRANGE
            var migration = new AddFetchTokenFieldToJobDtoStep();
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
                ["Parameters"] = new BsonDocument(),
                ["StateHistory"] = new BsonArray()
            };
            jobGraphCollection.InsertOne(jobDto);

            // Insert a non-JobDto document
            var otherDoc = new BsonDocument
            {
                ["_id"] = ObjectId.GenerateNewId(),
                ["_t"] = "SomeOtherDto",
                ["Name"] = "not-a-job"
            };
            jobGraphCollection.InsertOne(otherDoc);

            // ACT
            var result = migration.Execute(_database, _storageOptions, new MongoMigrationContext());

            // ASSERT
            Assert.True(result, "Expected migration to be successful");

            var updatedJob = jobGraphCollection.Find(new BsonDocument("_id", jobDto["_id"])).First();
            Assert.True(updatedJob.Contains("FetchToken"));

            var otherUntouched = jobGraphCollection.Find(new BsonDocument("_id", otherDoc["_id"])).First();
            Assert.False(otherUntouched.Contains("FetchToken"));
        }

        #endregion
    }
}
