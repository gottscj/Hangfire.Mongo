using System;
using System.Collections.Generic;
using System.Linq;
using Hangfire.Mongo.Database;
using Hangfire.Mongo.Migration;
using Hangfire.Mongo.Migration.Steps.Version15;
using Hangfire.Mongo.Tests.Utils;
using MongoDB.Bson;
using MongoDB.Driver;
using Xunit;

namespace Hangfire.Mongo.Tests.Migration
{
    [Collection("Database")]
    public class Version15MigrationStepFacts
    {
        private readonly HangfireDbContext _dbContext;
        private readonly IMongoDatabase _database;

        public Version15MigrationStepFacts(MongoIntegrationTestFixture fixture)
        {
            _dbContext = fixture.CreateDbContext();
            _database = _dbContext.Database;
        }

        [Fact]
        public void ExecuteStep00_ValidExistingLockDto_Success()
        {
            // ARRANGE
            var collection = _database.GetCollection<BsonDocument>("hangfire.locks");
            collection.Indexes.DropAll();
            var indexBuilder = Builders<BsonDocument>.IndexKeys;
            var indexNames = new[] {"Resource", "ExpireAt"};
            var indexModels = indexNames.Select(indexName =>
            {
                var index = indexBuilder.Descending(indexName);
                return new CreateIndexModel<BsonDocument>(index, new CreateIndexOptions
                {
                    Name = indexName,
                    Sparse = true
                });
            }).ToList();
            collection.Indexes.CreateMany(indexModels);

            // ACT
            var result = new CreateUniqueLockIndex().Execute(_dbContext.Database, new MongoStorageOptions(),
                new MongoMigrationContext());

            // ASSERT
            var indexes = collection.Indexes.List().ToList();
            AssertIndex(indexes, "Resource", true);
        }

        [Fact]
        public void ExecuteStep01_ValidExistingServerDto_Success()
        {
            // ARRANGE
            var json =
                @"
                { '_id' : 'test-server', 
                  'Data' : '{\'WorkerCount\':20,\'Queues\':[\'default\'],\'StartedAt\':\'2018-12-03T22:19:00Z\'}', 
                  'LastHeartbeat' : ISODate('2018-12-03T22:19:00.636Z')  
                }";
            var originalServerDto = BsonDocument.Parse(json);
            var document = BsonDocument.Parse(originalServerDto["Data"].AsString);
            var collection = _database.GetCollection<BsonDocument>("hangfire.server");
            collection.DeleteOne(new BsonDocument("_id", "test-server"));
            collection.InsertOne(originalServerDto);

            // ACT
            var result = new MakeServerDataEmbeddedDocument().Execute(_database, new MongoStorageOptions(),
                new MongoMigrationContext());

            // ASSERT
            var migratedServerDto = collection.Find(new BsonDocument()).Single();
            Assert.True(result, "Expected migration to be successful, reported 'false'");
            Assert.Equal(originalServerDto["_id"], migratedServerDto["_id"]);
            Assert.Equal(document["WorkerCount"].AsInt32, migratedServerDto["WorkerCount"].AsInt32);
            Assert.Equal(document["Queues"].AsBsonArray.Select(d => d.AsString).ToArray(),
                migratedServerDto["Queues"].AsBsonArray.Select(d => d.AsString).ToArray());
            Assert.Equal(DateTime.Parse(document["StartedAt"].AsString).ToUniversalTime(), migratedServerDto["StartedAt"].ToUniversalTime());
            Assert.Equal(originalServerDto["LastHeartbeat"], migratedServerDto["LastHeartbeat"].ToUniversalTime());
        }

        [Fact]
        public void ExecuteStep02_ValidExistingSetDto_Success()
        {
            // ARRANGE
            var collection = _database.GetCollection<BsonDocument>("hangfire.jobGraph");
            var originalSetDto = new BsonDocument
            {
                ["_id"] = ObjectId.GenerateNewId(),
                ["Key"] = "Key",
                ["Value"] = "Value",
                ["_t"] = "SetDto"
            };
            collection.DeleteMany(new BsonDocument("_t", "SetDto"));
            collection.InsertOne(originalSetDto);

            // ACT
            var result = new CreateCompositeKeys().Execute(_database, new MongoStorageOptions(), new MongoMigrationContext());

            // ASSERT
            var migratedSetDto = collection.Find(new BsonDocument("_t", "SetDto")).Single();
            Assert.True(result, "Expected migration to be successful, reported 'false'");
            Assert.Equal("Key:Value", migratedSetDto["Key"].AsString);
        }

        [Fact]
        public void ExecuteStep03_MultipleCountersNotDeleted_OldCountersDeleted()
        {
            // ARRANGE
            var collection = _database.GetCollection<BsonDocument>("hangfire.jobGraph");
            collection.Indexes.DropAll();
            collection.DeleteMany(new BsonDocument("_t", "CounterDto"));

            var counters = new List<BsonDocument>();
            foreach (var i in Enumerable.Range(0, 5))
            {
                counters.Add(new BsonDocument
                {
                    ["_id"] = ObjectId.GenerateNewId(),
                    ["Key"] = "stats:succeeded",
                    ["Value"] = 1L,
                    ["ExpireAt"] = BsonNull.Value,
                    ["_t"] = "CounterDto"
                });
            }

            var mergedCounter = new BsonDocument
            {
                ["_id"] = ObjectId.GenerateNewId(),
                ["Key"] = "stats:succeeded",
                ["Value"] = 5L,
                ["ExpireAt"] = BsonNull.Value,
                ["_t"] = "CounterDto"
            };
            counters.Add(mergedCounter);
            collection.InsertMany(counters);

            // ACT
            var result = new RemoveMergedCounters().Execute(_database, new MongoStorageOptions(), new MongoMigrationContext());

            // ASSERT
            var remainingCounter = collection.Find(new BsonDocument("_t", "CounterDto")).Single();
            Assert.True(result, "Expected migration to be successful, reported 'false'");
            Assert.Equal(5, remainingCounter["Value"].AsInt64);
        }

        [Fact]
        public void ExecuteStep03_MultipleCountersDifferentValues_CountersMerged()
        {
            // ARRANGE
            var collection = _database.GetCollection<BsonDocument>("hangfire.jobGraph");
            collection.Indexes.DropAll();
            collection.DeleteMany(new BsonDocument("_t", "CounterDto"));

            var counters = new List<BsonDocument>();
            foreach (var i in Enumerable.Range(0, 5))
            {
                counters.Add(new BsonDocument
                {
                    ["_id"] = ObjectId.GenerateNewId(),
                    ["Key"] = "stats:succeeded",
                    ["Value"] = 1L,
                    ["ExpireAt"] = BsonNull.Value,
                    ["_t"] = "CounterDto"
                });
            }

            var mergedCounter = new BsonDocument
            {
                ["_id"] = ObjectId.GenerateNewId(),
                ["Key"] = "stats:succeeded",
                ["Value"] = 5L,
                ["ExpireAt"] = BsonNull.Value,
                ["_t"] = "CounterDto"
            };
            var aggregatedCounter = new BsonDocument
            {
                ["_id"] = ObjectId.GenerateNewId(),
                ["Key"] = "stats:succeeded",
                ["Value"] = 5L,
                ["ExpireAt"] = BsonNull.Value,
                ["_t"] = "CounterDto"
            };
            counters.Add(mergedCounter);
            counters.Add(aggregatedCounter);
            collection.InsertMany(counters);

            // ACT
            var result = new RemoveMergedCounters().Execute(_database, new MongoStorageOptions(), new MongoMigrationContext());

            // ASSERT
            var remainingCounter = collection.Find(new BsonDocument("_t", "CounterDto")).Single();
            Assert.True(result, "Expected migration to be successful, reported 'false'");
            Assert.Equal(10, remainingCounter["Value"].AsInt64);
        }

        [Fact]
        public void ExecuteStep03_OneCounter_Nothing()
        {
            // ARRANGE
            var collection = _database.GetCollection<BsonDocument>("hangfire.jobGraph");

            collection.DeleteMany(new BsonDocument("Key", "stats:succeeded"));
            collection.InsertOne(new BsonDocument
            {
                ["_id"] = ObjectId.GenerateNewId(),
                ["Key"] = "stats:succeeded",
                ["Value"] = 1L,
                ["ExpireAt"] = BsonNull.Value,
                ["_t"] = "CounterDto"
            });

            // ACT
            var result = new RemoveMergedCounters().Execute(_database, new MongoStorageOptions(), new MongoMigrationContext());

            // ASSERT
            var remainingCounter = collection.Find(new BsonDocument("_t", "CounterDto")).Single();
            Assert.NotNull(remainingCounter);
            Assert.True(result, "Expected migration to be successful, reported 'false'");
        }

        [Fact]
        public void ExecuteStep03_TwoCountersSameValue_NewestChosen()
        {
            // ARRANGE
            var collection = _database.GetCollection<BsonDocument>("hangfire.jobGraph");
            collection.Indexes.DropAll();
            collection.DeleteMany(new BsonDocument("_t", "CounterDto"));
            collection.InsertOne(new BsonDocument
            {
                ["_id"] = ObjectId.GenerateNewId(),
                ["Key"] = "stats:succeeded",
                ["Value"] = 1L,
                ["ExpireAt"] = BsonNull.Value,
                ["_t"] = "CounterDto"
            });
            var expectedDoc = new BsonDocument
            {
                ["_id"] = ObjectId.GenerateNewId(),
                ["Key"] = "stats:succeeded",
                ["Value"] = 1L,
                ["ExpireAt"] = BsonNull.Value,
                ["_t"] = "CounterDto"
            };

            collection.InsertOne(expectedDoc);

            // ACT
            var result = new RemoveMergedCounters().Execute(_database, new MongoStorageOptions(), new MongoMigrationContext());

            // ASSERT
            var remainingCounter = collection.Find(new BsonDocument("_t", "CounterDto")).Single();
            Assert.NotNull(remainingCounter);
            Assert.True(result, "Expected migration to be successful, reported 'false'");
            Assert.Equal(expectedDoc["_id"], remainingCounter["_id"]);
        }

        [Fact]
        public void ExecuteStep04_UpdateListDtoKeySchema_Success()
        {
            // ARRANGE
            var collection = _database.GetCollection<BsonDocument>("hangfire.jobGraph");

            collection.DeleteMany(new BsonDocument("_t", "ListDto"));
            collection.InsertOne(new BsonDocument
            {
                ["_id"] = ObjectId.GenerateNewId(),
                ["Key"] = "list-key",
                ["Value"] = "Value",
                ["ExpireAt"] = BsonNull.Value,
                ["_t"] = "ListDto"
            });

            // ACT
            var result = new UpdateListDtoKeySchema().Execute(_database, new MongoStorageOptions(), new MongoMigrationContext());

            // ASSERT
            var listDto = collection.Find(new BsonDocument("_t", "ListDto")).Single();
            Assert.NotNull(listDto);
            Assert.True(result, "Expected migration to be successful, reported 'false'");
            Assert.True(listDto.Contains("Item"));
            Assert.False(listDto.Contains("Key"));
            Assert.Equal(BsonArray.Create(new[]{"BaseJobDto", "ExpiringJobDto", "ListDto"}), listDto["_t"]);
        }

        [Fact]
        public void ExecuteStep05_UpdateIndexes_Success()
        {
            // ARRANGE
            var collection = _database.GetCollection<BsonDocument>("hangfire.jobGraph");

            collection.Indexes.DropAll();

            // ACT
            var result = new UpdateIndexes().Execute(_database, new MongoStorageOptions(), new MongoMigrationContext());

            // ASSERT
            Assert.True(result, "Expected migration to be successful, reported 'false'");
            var indexes = collection.Indexes.List().ToList();
            AssertIndex(indexes, "Key", true, descending: false);
            AssertIndex(indexes, "StateName", false);
            AssertIndex(indexes, "ExpireAt", false);
            AssertIndex(indexes, "_t", false);
            AssertIndex(indexes, "Queue", false);
            AssertIndex(indexes, "FetchedAt", false);
            AssertIndex(indexes, "Value", false);
            AssertIndex(indexes, "Item", false);
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