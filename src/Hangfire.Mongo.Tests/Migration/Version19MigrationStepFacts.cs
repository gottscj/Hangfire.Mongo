using System;
using System.Linq;
using Hangfire.Mongo.Migration;
using Hangfire.Mongo.Migration.Steps.Version19;
using Hangfire.Mongo.Tests.Utils;
using MongoDB.Bson;
using MongoDB.Driver;
using Xunit;

namespace Hangfire.Mongo.Tests.Migration
{
    [Collection("Database")]
    public class Version19MigrationStepFacts
    {
        private readonly IMongoDatabase _database;
        private readonly Random _random;
        private readonly AddTypeToSetDto _addTypeToSetDto;
        public Version19MigrationStepFacts(MongoIntegrationTestFixture fixture)
        {
            var dbContext = fixture.CreateDbContext();
            _database = dbContext.Database;
            _random = new Random();
            _addTypeToSetDto = new AddTypeToSetDto();
        }

        [Fact]
        public void ExecuteStep01_AddTypeToSetDto_Success()
        {
            // ARRANGE
            var collection = _database.GetCollection<BsonDocument>("hangfire.jobGraph");

            collection.Indexes.DropAll();
            collection.InsertMany(new []
            {
                CreateSetDto(),
                CreateSetDto(),
                CreateSetDto(),
                CreateSetDto(),
            });
            // ACT
            var result = _addTypeToSetDto.Execute(_database, new MongoStorageOptions(), new MongoMigrationContext());

            // ASSERT
            Assert.True(result, "Expected migration to be successful, reported 'false'");
            var migrated = collection.Find(new BsonDocument("_t", "SetDto")).ToList();
            foreach (var doc in migrated)
            {
                Assert.True(doc.Contains("SetType"));
            }

            var index = collection.Indexes.List().ToList().FirstOrDefault(b => b["name"].AsString == "SetType");
            Assert.NotNull(index);
        }

        public BsonDocument CreateSetDto()
        {
            var value = _random.Next(123, 234);
            return new BsonDocument
            {
                ["Key"] = $"schedule<{value}>",
                ["Score"] = (double) _random.Next(12124124, 193435467),
                ["Value"] = value.ToString(),
                ["ExpireAt"] = DateTime.UtcNow.AddDays(2),
                ["_t"] = "SetDto",
                ["_id"] = ObjectId.GenerateNewId()
            };
        }
    }
}