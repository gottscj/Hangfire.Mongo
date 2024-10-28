using Hangfire.Mongo.Database;
using Hangfire.Mongo.Migration;
using Hangfire.Mongo.Migration.Steps.Version16;
using Hangfire.Mongo.Tests.Utils;
using MongoDB.Bson;
using MongoDB.Driver;
using Xunit;

namespace Hangfire.Mongo.Tests.Migration
{
    [Collection("Database")]
    public class Version16MigrationStepFacts
    {
        private readonly HangfireDbContext _dbContext;
        private readonly IMongoDatabase _database;

        public Version16MigrationStepFacts(MongoIntegrationTestFixture fixture)
        {
            _dbContext = fixture.CreateDbContext();
            _database = _dbContext.Database;
        }

        [Fact]
        public void ExecuteStep00_SetDtoNotContainingValue_Success()
        {
            // ARRANGE
            var collection = _database.GetCollection<BsonDocument>("hangfire.jobGraph");
            collection.DeleteMany(_ => true);
            var setDtoJson = @"
                    {
                        '_id' : ObjectId('5c2d1b398dfc0236031e1a35'), 
                        'Key' : 'recurring-jobs:HomeController.PrintToDebug', 
                        '_t' : ['BaseJobDto', 'ExpiringJobDto','KeyJobDto','SetDto'],
                        'ExpireAt' : null,
                        'Score' : 0.0, 
                        'Value' : null
                    }";

            collection.InsertOne(BsonDocument.Parse(setDtoJson));

            // ACT
            var result = new UpdateSetDtoKeyAndValueField().Execute(_dbContext.Database, new MongoStorageOptions(),
                new MongoMigrationContext());

            // ASSERT
            var migratedSetDto = collection.Find(_ => true).Single();
            Assert.Equal("recurring-jobs<HomeController.PrintToDebug>", migratedSetDto["Key"].AsString);
            Assert.Equal("HomeController.PrintToDebug", migratedSetDto["Value"].AsString);
        }

        [Fact]
        public void ExecuteStep00_SetDtoNotCompositeKey_Success()
        {
            // ARRANGE
            var collection = _database.GetCollection<BsonDocument>("hangfire.jobGraph");
            collection.DeleteMany(_ => true);
            var setDtoJson = @"
                    {
                        '_id' : ObjectId('5c2d1b398dfc0236031e1a35'), 
                        'Key' : 'recurring-jobs', 
                        '_t' : ['BaseJobDto', 'ExpiringJobDto','KeyJobDto','SetDto'],
                        'ExpireAt' : null,
                        'Score' : 0.0, 
                        'Value' : 'HomeController.PrintToDebug'
                    }";

            collection.InsertOne(BsonDocument.Parse(setDtoJson));

            // ACT
            var result = new UpdateSetDtoKeyAndValueField().Execute(_dbContext.Database, new MongoStorageOptions(),
                new MongoMigrationContext());

            // ASSERT
            var migratedSetDto = collection.Find(_ => true).Single();
            Assert.Equal("recurring-jobs<HomeController.PrintToDebug>", migratedSetDto["Key"].AsString);
            Assert.Equal("HomeController.PrintToDebug", migratedSetDto["Value"].AsString);
        }
    }
}