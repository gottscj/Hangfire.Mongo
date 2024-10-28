using System.Collections.Generic;
using System.Linq;
using Hangfire.Mongo.Migration;
using Hangfire.Mongo.Migration.Steps.Version18;
using Hangfire.Mongo.Tests.Utils;
using MongoDB.Bson;
using MongoDB.Driver;
using Xunit;

namespace Hangfire.Mongo.Tests.Migration
{
    [Collection("Database")]
    public class Version18MigrationStepFacts
    {
        private readonly IMongoDatabase _database;

        public Version18MigrationStepFacts(MongoIntegrationTestFixture fixture)
        {
            var dbContext = fixture.CreateDbContext();
            _database = dbContext.Database;
        }

        [Fact]
        public void ExecuteStep01_UpdateIndexes_Success()
        {
            // ARRANGE
            var collection = _database.GetCollection<BsonDocument>("hangfire.jobGraph");

            collection.Indexes.DropAll();

            // ACT
            var result = new UpdateIndexes().Execute(_database, 
                new MongoStorageOptions(), new MongoMigrationContext());

            // ASSERT
            Assert.True(result, "Expected migration to be successful, reported 'false'");
            var indexes = collection.Indexes.List().ToList();
            AssertIndex(indexes, "Score", false, descending: false);
        }

        private static void AssertIndex(IList<BsonDocument> indexes, string indexName, bool unique ,bool descending = true)
        {
            var index = indexes.FirstOrDefault(d => d["name"].Equals(indexName));
            Assert.True(index != null, $"Expected '{indexName}' field to be indexed");
            if (unique)
            {
                Assert.True(index.Contains("unique"), "Expected 'unique' field to be present");
                Assert.True(index["unique"].Equals(true), "Expected 'unique' field to be 'true'");
            }

            if (descending)
            {
                Assert.True(index["key"][indexName].AsInt32 == -1, "Expected index to be 'Descending'");
            }
            else
            {
                Assert.True(index["key"][indexName].AsInt32 == 1, "Expected index to be 'Descending'");
            }
        }

    }
}