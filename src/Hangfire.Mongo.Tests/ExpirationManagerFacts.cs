using Hangfire.Mongo.Database;
using Hangfire.Mongo.Dto;
using Hangfire.Mongo.MongoUtils;
using Hangfire.Mongo.Tests.Utils;
using MongoDB.Bson;
using MongoDB.Driver;
using System;
using System.Threading;
using Xunit;

namespace Hangfire.Mongo.Tests
{
#pragma warning disable 1591
    [Collection("Database")]
    public class ExpirationManagerFacts
    {
        private readonly MongoStorage _storage;

        private readonly CancellationToken _token;

        public ExpirationManagerFacts()
        {
            _storage = new MongoStorage(ConnectionUtils.GetConnectionString(), ConnectionUtils.GetDatabaseName());
            _token = new CancellationToken(true);
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenStorageIsNull()
        {
            Assert.Throws<ArgumentNullException>(() => new ExpirationManager(null));
        }

        [Fact, CleanDatabase]
        public void Execute_RemovesOutdatedRecords()
        {
            using (var connection = ConnectionUtils.CreateConnection())
            {
                var entryId = CreateExpirationEntry(connection, connection.GetServerTimeUtc().AddMonths(-1));
                var manager = CreateManager();

                manager.Execute(_token);

                Assert.True(IsEntryExpired(connection, entryId));
            }
        }

        [Fact, CleanDatabase]
        public void Execute_DoesNotRemoveEntries_WithNoExpirationTimeSet()
        {
            using (var connection = ConnectionUtils.CreateConnection())
            {
                var entryId = CreateExpirationEntry(connection, null);
                var manager = CreateManager();

                manager.Execute(_token);

                Assert.False(IsEntryExpired(connection, entryId));
            }
        }

        [Fact, CleanDatabase]
        public void Execute_DoesNotRemoveEntries_WithFreshExpirationTime()
        {
            using (var connection = ConnectionUtils.CreateConnection())
            {
                var entryId = CreateExpirationEntry(connection, DateTime.Now.AddMonths(1));
                var manager = CreateManager();

                manager.Execute(_token);

                Assert.False(IsEntryExpired(connection, entryId));
            }
        }

        [Fact, CleanDatabase]
        public void Execute_Processes_CounterTable()
        {
            using (var connection = ConnectionUtils.CreateConnection())
            {
                // Arrange
                connection.Counter.InsertOne(new CounterDto
                {
                    Id = ObjectId.GenerateNewId(),
                    Key = "key",
                    Value = 1,
                    ExpireAt = connection.GetServerTimeUtc().AddMonths(-1)
                });

                var manager = CreateManager();

                // Act
                manager.Execute(_token);

                // Assert
                var count = connection.Counter.Count(new BsonDocument());
                Assert.Equal(0, count);
            }
        }

        [Fact, CleanDatabase]
        public void Execute_Processes_JobTable()
        {
            using (var connection = ConnectionUtils.CreateConnection())
            {
                // Arrange
                connection.Job.InsertOne(new JobDto
                {
                    Id = 1,
                    InvocationData = "",
                    Arguments = "",
                    CreatedAt = connection.GetServerTimeUtc(),
                    ExpireAt = connection.GetServerTimeUtc().AddMonths(-1),
                });

                var manager = CreateManager();

                // Act
                manager.Execute(_token);

                // Assert
                var count = connection.Job.Count(new BsonDocument());
                Assert.Equal(0, count);
            }
        }

        [Fact, CleanDatabase]
        public void Execute_Processes_ListTable()
        {
            using (var connection = ConnectionUtils.CreateConnection())
            {
                // Arrange
                connection.List.InsertOne(new ListDto
                {
                    Id = ObjectId.GenerateNewId(),
                    Key = "key",
                    ExpireAt = connection.GetServerTimeUtc().AddMonths(-1)
                });

                var manager = CreateManager();

                // Act
                manager.Execute(_token);

                // Assert
                var count = connection.List.Count(new BsonDocument());
                Assert.Equal(0, count);
            }
        }

        [Fact, CleanDatabase]
        public void Execute_Processes_SetTable()
        {
            using (var connection = ConnectionUtils.CreateConnection())
            {
                // Arrange
                connection.Set.InsertOne(new SetDto
                {
                    Id = ObjectId.GenerateNewId(),
                    Key = "key",
                    Score = 0,
                    Value = "",
                    ExpireAt = connection.GetServerTimeUtc().AddMonths(-1)
                });

                var manager = CreateManager();

                // Act
                manager.Execute(_token);

                // Assert
                var count = connection.Set.Count(new BsonDocument());
                Assert.Equal(0, count);
            }
        }

        [Fact, CleanDatabase]
        public void Execute_Processes_HashTable()
        {
            using (var connection = ConnectionUtils.CreateConnection())
            {
                // Arrange
                connection.Hash.InsertOne(new HashDto
                {
                    Id = ObjectId.GenerateNewId(),
                    Key = "key",
                    Field = "field",
                    Value = "",
                    ExpireAt = connection.GetServerTimeUtc().AddMonths(-1)
                });

                var manager = CreateManager();

                // Act
                manager.Execute(_token);

                // Assert
                var count = connection.Hash.Count(new BsonDocument());
                Assert.Equal(0, count);
            }
        }


        [Fact, CleanDatabase]
        public void Execute_Processes_AggregatedCounterTable()
        {
            using (var connection = ConnectionUtils.CreateConnection())
            {
                // Arrange
                connection.AggregatedCounter.InsertOne(new AggregatedCounterDto
                {
                    Key = "key",
                    Value = 1,
                    ExpireAt = DateTime.UtcNow.AddMonths(-1)
                });

                var manager = CreateManager();

                // Act
                manager.Execute(_token);

                // Assert
                Assert.Equal(0, connection.Counter.Find(new BsonDocument()).Count());
            }
        }



        private static ObjectId CreateExpirationEntry(HangfireDbContext connection, DateTime? expireAt)
        {
            var counter = new AggregatedCounterDto
            {
                Id = ObjectId.GenerateNewId(),
                Key = "key",
                Value = 1,
                ExpireAt = expireAt
            };
            connection.AggregatedCounter.InsertOne(counter);

            var id = counter.Id;

            return id;
        }

        private static bool IsEntryExpired(HangfireDbContext connection, ObjectId entryId)
        {
            var count = connection.AggregatedCounter.Find(Builders<AggregatedCounterDto>.Filter.Eq(_ => _.Id, entryId)).Count();
            return count == 0;
        }

        private ExpirationManager CreateManager()
        {
            return new ExpirationManager(_storage);
        }
    }
#pragma warning restore 1591
}