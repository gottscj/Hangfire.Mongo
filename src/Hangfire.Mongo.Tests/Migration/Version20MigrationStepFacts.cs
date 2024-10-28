using System;
using System.Collections.Generic;
using System.Linq;
using Hangfire.Mongo.Migration;
using Hangfire.Mongo.Migration.Steps;
using Hangfire.Mongo.Migration.Steps.Version20;
using Hangfire.Mongo.Tests.Utils;
using MongoDB.Bson;
using MongoDB.Driver;
using Xunit;

namespace Hangfire.Mongo.Tests.Migration
{
    [Collection("Database")]
    public class Version20MigrationStepFacts
    {
        private readonly IMongoDatabase _database;
        private readonly IMongoMigrationStep _migration;
        public Version20MigrationStepFacts(MongoIntegrationTestFixture fixture)
        {
            var dbContext = fixture.CreateDbContext();
            _database = dbContext.Database;
            _migration = new RemoveJobQueueDto();
        }

        [Fact]
        public void ExecuteStep01_MergeJobQueueIntoJobDto_Success()
        {
            // ARRANGE
            var collection = _database.GetCollection<BsonDocument>("hangfire.jobGraph");
            collection.DeleteMany("{}");
            collection.InsertMany(CreateJobQueueDtos());

            // ACT
            var result = _migration.Execute(_database, new MongoStorageOptions(), new MongoMigrationContext());

            // ASSERT
            Assert.True(result, "Expected migration to be successful, reported 'false'");
            var migrated = collection.Find(new BsonDocument("_t", "JobDto")).ToList();
            foreach (var doc in migrated)
            {
                Assert.True(doc.Contains("Queue"));
                Assert.True(doc.Contains("FetchedAt"));
            }
            var migratedFrom = collection.Find(new BsonDocument("_t", "JobQueueDto")).ToList();

            Assert.Empty(migratedFrom);
        }

        [Fact]
        public void ExecuteStep01_AlreadyRunJobsGetsFetchedAtSet_Success()
        {
            // ARRANGE
            var collection = _database.GetCollection<BsonDocument>("hangfire.jobGraph");
            collection.DeleteMany("{}");
            var jobs = CreateJobQueueDtos(jobQueueCount: 5, jobDtoCount: 3);
            collection.InsertMany(jobs);

            // ACT
            var result = _migration.Execute(_database, new MongoStorageOptions(), new MongoMigrationContext());

            // ASSERT
            Assert.True(result, "Expected migration to be successful, reported 'false'");
            var migrated = collection.Find(new BsonDocument("_t", "JobDto")).ToList();
            foreach (var doc in migrated)
            {
                Assert.True(doc.Contains("Queue"));
                Assert.True(doc.Contains("FetchedAt"));
            }
            var migratedFrom = collection.Find(new BsonDocument("_t", "JobQueueDto")).ToList();
            Assert.Empty(migratedFrom);

            var fetchedJobs = migrated.Where(b => b["FetchedAt"] != BsonNull.Value).ToList();
            foreach (var item in fetchedJobs)
            {
                var job = jobs.Find(j => j["_id"] == item["_id"]);
                Assert.Equal(job["StateHistory"][0]["CreatedAt"], item["FetchedAt"]);
            }
        }

        [Fact]
        public void ExecuteStep02_AddIndexes_Success()
        {
            // ARRANGE
            var collection = _database.GetCollection<BsonDocument>("hangfire.jobGraph");
            collection.DeleteMany("{}");
            collection.InsertMany(CreateJobQueueDtos(0, 5));
            var migration = new CompoundIndexes();
            // ACT
            var result = migration.Execute(_database, new MongoStorageOptions(), new MongoMigrationContext());

            // ASSERT
            var index = collection.Indexes.List().ToList().FirstOrDefault(b => b["name"].AsString == "IX_SetType_T_Score");
            var index2 = collection.Indexes.List().ToList().FirstOrDefault(b => b["name"].AsString == "T_ExpireAt");
            Assert.NotNull(index);
            Assert.NotNull(index2);
        }

        public List<BsonDocument> CreateJobQueueDtos(int jobQueueCount = 5, int jobDtoCount = 5)
        {
            var list = new List<BsonDocument>();
            var count = new[] { jobQueueCount, jobDtoCount }.Max();

            for (int i = 0; i < count; i++)
            {
                BsonDocument jobQueueDto = null;
                if (jobQueueCount > i)
                {
                    jobQueueDto = new BsonDocument
                    {
                        ["FetchedAt"] = BsonNull.Value,
                        ["Queue"] = "default",
                        ["JobId"] = ObjectId.GenerateNewId(),
                        ["_t"] = "JobQueueDto",
                        ["_id"] = ObjectId.GenerateNewId()
                    };
                    list.Add(jobQueueDto);
                }

                if (jobDtoCount > i)
                {
                    var jobDto = new BsonDocument
                    {
                        ["JobId"] = jobQueueDto != null ? jobQueueDto["_id"] : ObjectId.GenerateNewId(),
                        ["_t"] = "JobDto",
                        ["_id"] = ObjectId.GenerateNewId(),
                        ["StateName"] = "Enqueued",
                        ["StateHistory"] = new BsonArray
                        {
                            new BsonDocument
                            {
                                ["CreatedAt"] = DateTime.UtcNow.AddDays(-1)
                            }
                        }
                    };
                    list.Add(jobDto);
                }
            }
            return list;
        }

    }
}