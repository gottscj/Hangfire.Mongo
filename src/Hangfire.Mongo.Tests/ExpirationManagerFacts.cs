using System;
using System.Collections.Generic;
using System.Threading;
using Hangfire.Mongo.Database;
using Hangfire.Mongo.Dto;
using Hangfire.Mongo.Tests.Utils;
using MongoDB.Bson;
using Xunit;

namespace Hangfire.Mongo.Tests
{
#pragma warning disable 1591
    [Collection("Database")]
    public class ExpirationManagerFacts : IDisposable
    {
        private readonly HangfireDbContext _dbContext;
        private readonly CancellationToken _token;

        public ExpirationManagerFacts(MongoIntegrationTestFixture fixture)
        {
            fixture.CleanDatabase();
            _dbContext = fixture.CreateDbContext();

            _token = new CancellationToken(true);
        }

        [Fact]
        public void Execute_RemovesOutdatedRecords()
        {
            CreateExpirationEntries(_dbContext, DateTime.UtcNow.AddMonths(-1));
            var manager = CreateManager();

            manager.Execute(_token);

            Assert.True(IsEntryExpired(_dbContext));
        }

        [Fact]
        public void Execute_DoesNotRemoveEntries_WithNoExpirationTimeSet()
        {
            CreateExpirationEntries(_dbContext, null);
            var manager = CreateManager();

            manager.Execute(_token);

            Assert.False(IsEntryExpired(_dbContext));
        }

        [Fact]
        public void Execute_DoesNotRemoveEntries_WithFreshExpirationTime()
        {
            CreateExpirationEntries(_dbContext, DateTime.UtcNow.AddMonths(1));
            var manager = CreateManager();

            manager.Execute(_token);


            Assert.False(IsEntryExpired(_dbContext));
        }

        [Fact]
        public void Execute_Processes_CounterTable()
        {
            // Arrange
            _dbContext.JobGraph.InsertOne(new CounterDto
            {
                Id = ObjectId.GenerateNewId(),
                Key = "key",
                Value = 1L,
                ExpireAt = DateTime.UtcNow.AddMonths(-1)
            }.Serialize());

            var manager = CreateManager();

            // Act
            manager.Execute(_token);

            // Assert
            var count = _dbContext.JobGraph.Count(new BsonDocument("_t", nameof(CounterDto)));
            Assert.Equal(0, count);
        }

        [Fact]
        public void Execute_Processes_JobTable()
        {
            // Arrange
            _dbContext.JobGraph.InsertOne(new JobDto
            {
                Id = ObjectId.GenerateNewId(),
                InvocationData = "",
                Arguments = "",
                CreatedAt = DateTime.UtcNow,
                ExpireAt = DateTime.UtcNow.AddMonths(-1),
            }.Serialize());

            var manager = CreateManager();

            // Act
            manager.Execute(_token);

            // Assert
            var count = _dbContext.JobGraph.Count(new BsonDocument("_t", nameof(JobDto)));
            Assert.Equal(0, count);
        }

        [Fact]
        public void Execute_Processes_ListTable()
        {
            // Arrange
            _dbContext.JobGraph.InsertOne(new ListDto
            {
                Id = ObjectId.GenerateNewId(),
                Item = "key",
                ExpireAt = DateTime.UtcNow.AddMonths(-1)
            }.Serialize());

            var manager = CreateManager();

            // Act
            manager.Execute(_token);

            // Assert
            var count = _dbContext
                .JobGraph
                .Count(new BsonDocument("_t", nameof(ListDto)));
            Assert.Equal(0, count);
        }

        [Fact]
        public void Execute_Processes_SetTable()
        {
            // Arrange
            var setDto = new SetDto
            {
                Id = ObjectId.GenerateNewId(),
                Key = "key<>",
                Score = 0,
                ExpireAt = DateTime.UtcNow.AddMonths(-1)
            }.Serialize();
            _dbContext.JobGraph.InsertOne(setDto);

            var manager = CreateManager();

            // Act
            manager.Execute(_token);

            // Assert
            var count = _dbContext
                .JobGraph
                .Count(new BsonDocument("_t", nameof(SetDto)));
            Assert.Equal(0, count);
        }

        [Fact]
        public void Execute_Processes_HashTable()
        {
            // Arrange
            _dbContext.JobGraph.InsertOne(new HashDto
            {
                Id = ObjectId.GenerateNewId(),
                Key = "key",
                Fields = new Dictionary<string, string> {["field"] = ""},
                ExpireAt = DateTime.UtcNow.AddMonths(-1)
            }.Serialize());

            var manager = CreateManager();

            // Act
            manager.Execute(_token);

            // Assert
            var count = _dbContext
                .JobGraph
                .Count(new BsonDocument("_t", nameof(HashDto)));
            Assert.Equal(0, count);
        }


        [Fact]
        public void Execute_Processes_AggregatedCounterTable()
        {
            // Arrange
            _dbContext.JobGraph.InsertOne(new CounterDto
            {
                Key = "key",
                Value = 1,
                ExpireAt = DateTime.UtcNow.AddMonths(-1)
            }.Serialize());

            var manager = CreateManager();

            // Act
            manager.Execute(_token);

            // Assert
            Assert.Equal(0, _dbContext
                .JobGraph
                .Count(new BsonDocument("_t", nameof(CounterDto))));
        }

        [Fact]
        public void Execute_Processes_StateHistoryTable_RemovesExpiredEntries()
        {
            // Arrange
            var jobId = ObjectId.GenerateNewId();
            var expiredJobDto = new JobDto
            {
                Id = jobId,
                InvocationData = "",
                Arguments = "",
                CreatedAt = DateTime.UtcNow,
                ExpireAt = DateTime.UtcNow.AddMonths(-1)
            };
            _dbContext.JobGraph.InsertOne(expiredJobDto.Serialize());

            // Add StateHistory entries for the expired job
            _dbContext.StateHistory.InsertOne(new JobStateHistoryDto
            {
                JobId = jobId,
                State = new StateDto { Name = "Succeeded" }
            }.Serialize());

            _dbContext.StateHistory.InsertOne(new JobStateHistoryDto
            {
                JobId = jobId,
                State = new StateDto { Name = "Processing" }
            }.Serialize());

            var manager = CreateManager();

            // Act
            manager.Execute(_token);

            // Assert
            var stateHistoryCount = _dbContext.StateHistory.Count(new BsonDocument(nameof(JobStateHistoryDto.JobId), jobId));
            Assert.Equal(0, stateHistoryCount);
        }

        [Fact]
        public void Execute_Processes_StateHistoryTable_KeepsNonExpiredEntries()
        {
            // Arrange
            var jobId = ObjectId.GenerateNewId();
            var nonExpiredJobDto = new JobDto
            {
                Id = jobId,
                InvocationData = "",
                Arguments = "",
                CreatedAt = DateTime.UtcNow,
                ExpireAt = DateTime.UtcNow.AddMonths(1)
            };
            _dbContext.JobGraph.InsertOne(nonExpiredJobDto.Serialize());

            // Add StateHistory entries for the non-expired job
            _dbContext.StateHistory.InsertOne(new JobStateHistoryDto
            {
                JobId = jobId,
                State = new StateDto { Name = "Succeeded" }
            }.Serialize());

            var manager = CreateManager();

            // Act
            manager.Execute(_token);

            // Assert
            var stateHistoryCount = _dbContext.StateHistory.Count(new BsonDocument(nameof(JobStateHistoryDto.JobId), jobId));
            Assert.Equal(1, stateHistoryCount);
        }

        [Fact]
        public void Execute_Removes_StateHistoryAndJobEntries_Together()
        {
            // Arrange
            var expiredJobId = ObjectId.GenerateNewId();
            var nonExpiredJobId = ObjectId.GenerateNewId();

            // Insert expired job
            _dbContext.JobGraph.InsertOne(new JobDto
            {
                Id = expiredJobId,
                InvocationData = "",
                Arguments = "",
                CreatedAt = DateTime.UtcNow,
                ExpireAt = DateTime.UtcNow.AddMonths(-1)
            }.Serialize());

            // Insert non-expired job
            _dbContext.JobGraph.InsertOne(new JobDto
            {
                Id = nonExpiredJobId,
                InvocationData = "",
                Arguments = "",
                CreatedAt = DateTime.UtcNow,
                ExpireAt = DateTime.UtcNow.AddMonths(1)
            }.Serialize());

            // Add StateHistory entries for both jobs
            _dbContext.StateHistory.InsertOne(new JobStateHistoryDto
            {
                JobId = expiredJobId,
                State = new StateDto { Name = "Succeeded" }
            }.Serialize());

            _dbContext.StateHistory.InsertOne(new JobStateHistoryDto
            {
                JobId = nonExpiredJobId,
                State = new StateDto { Name = "Succeeded" }
            }.Serialize());

            var manager = CreateManager();

            // Act
            manager.Execute(_token);

            // Assert - expired job and its history should be deleted
            var expiredJobCount = _dbContext.JobGraph.Count(new BsonDocument("_id", expiredJobId));
            Assert.Equal(0, expiredJobCount);

            var expiredHistoryCount = _dbContext.StateHistory.Count(new BsonDocument(nameof(JobStateHistoryDto.JobId), expiredJobId));
            Assert.Equal(0, expiredHistoryCount);

            // Non-expired job and its history should remain
            var nonExpiredJobCount = _dbContext.JobGraph.Count(new BsonDocument("_id", nonExpiredJobId));
            Assert.Equal(1, nonExpiredJobCount);

            var nonExpiredHistoryCount = _dbContext.StateHistory.Count(new BsonDocument(nameof(JobStateHistoryDto.JobId), nonExpiredJobId));
            Assert.Equal(1, nonExpiredHistoryCount);
        }



        private static void CreateExpirationEntries(HangfireDbContext connection, DateTime? expireAt)
        {
            Commit(connection, x => x.AddToSet("my-key", "my-value"));
            Commit(connection, x => x.AddToSet("my-key", "my-value1"));
            Commit(connection, x => x.SetRangeInHash("my-hash-key", new[] { new KeyValuePair<string, string>("key", "value"), new KeyValuePair<string, string>("key1", "value1") }));
            Commit(connection, x => x.AddRangeToSet("my-key", new[] { "my-value", "my-value1" }));

            if (expireAt.HasValue)
            {
                var expireIn = expireAt.Value - DateTime.UtcNow;
                Commit(connection, x => x.ExpireHash("my-hash-key", expireIn));
                Commit(connection, x => x.ExpireSet("my-key", expireIn));
            }
        }

        private static bool IsEntryExpired(HangfireDbContext connection)
        {
            var count = connection
                .JobGraph
                .Count(new BsonDocument("_t", nameof(ExpiringJobDto)));

            return count == 0;
        }

        private MongoExpirationManager CreateManager()
        {
            return new MongoExpirationManager(_dbContext, new MongoStorageOptions());
        }

        private static void Commit(HangfireDbContext connection, Action<MongoWriteOnlyTransaction> action)
        {
            using (var transaction = new MongoWriteOnlyTransaction(connection, new MongoStorageOptions()))
            {
                action(transaction);
                transaction.Commit();
            }
        }

        public void Dispose()
        {
        }
    }
#pragma warning restore 1591
}