using Hangfire.Mongo.Database;
using Hangfire.Mongo.Dto;
using Hangfire.Mongo.MongoUtils;
using Hangfire.Mongo.Tests.Utils;
using MongoDB.Bson;
using MongoDB.Driver;
using System;
using System.Threading;
using Hangfire.Mongo.Helpers;
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
                AsyncHelper.RunSync(() => connection.Counter.InsertOneAsync(new CounterDto
                {
                    Id = ObjectId.GenerateNewId(),
                    Key = "key",
                    Value = 1,
                    ExpireAt = connection.GetServerTimeUtc().AddMonths(-1)
                }));

                var manager = CreateManager();

                // Act
                manager.Execute(_token);

                // Assert
                var count = AsyncHelper.RunSync(() => connection.Counter.CountAsync(new BsonDocument()));
                Assert.Equal(0, count);
            }
        }

        [Fact, CleanDatabase]
        public void Execute_Processes_JobTable()
        {
            using (var connection = ConnectionUtils.CreateConnection())
            {
                // Arrange
                AsyncHelper.RunSync(() => connection.Job.InsertOneAsync(new JobDto
                {
                    Id = 1,
                    InvocationData = "",
                    Arguments = "",
                    CreatedAt = connection.GetServerTimeUtc(),
                    ExpireAt = connection.GetServerTimeUtc().AddMonths(-1),
                }));

                var manager = CreateManager();

                // Act
                manager.Execute(_token);

                // Assert
                var count = AsyncHelper.RunSync(() => connection.Job.CountAsync(new BsonDocument()));
                Assert.Equal(0, count);
            }
        }

        [Fact, CleanDatabase]
        public void Execute_Processes_ListTable()
        {
            using (var connection = ConnectionUtils.CreateConnection())
            {
                // Arrange
                AsyncHelper.RunSync(() => connection.List.InsertOneAsync(new ListDto
                {
                    Id = ObjectId.GenerateNewId(),
                    Key = "key",
                    ExpireAt = connection.GetServerTimeUtc().AddMonths(-1)
                }));

                var manager = CreateManager();

                // Act
                manager.Execute(_token);

                // Assert
                var count = AsyncHelper.RunSync(() => connection.List.CountAsync(new BsonDocument()));
                Assert.Equal(0, count);
            }
        }

        [Fact, CleanDatabase]
        public void Execute_Processes_SetTable()
        {
            using (var connection = ConnectionUtils.CreateConnection())
            {
                // Arrange
                AsyncHelper.RunSync(() => connection.Set.InsertOneAsync(new SetDto
                {
                    Id = ObjectId.GenerateNewId(),
                    Key = "key",
                    Score = 0,
                    Value = "",
                    ExpireAt = connection.GetServerTimeUtc().AddMonths(-1)
                }));

                var manager = CreateManager();

                // Act
                manager.Execute(_token);

                // Assert
                var count = AsyncHelper.RunSync(() => connection.Set.CountAsync(new BsonDocument()));
                Assert.Equal(0, count);
            }
        }

        [Fact, CleanDatabase]
        public void Execute_Processes_HashTable()
        {
            using (var connection = ConnectionUtils.CreateConnection())
            {
                // Arrange
                AsyncHelper.RunSync(() => connection.Hash.InsertOneAsync(new HashDto
                {
                    Id = ObjectId.GenerateNewId(),
                    Key = "key",
                    Field = "field",
                    Value = "",
                    ExpireAt = connection.GetServerTimeUtc().AddMonths(-1)
                }));

                var manager = CreateManager();

                // Act
                manager.Execute(_token);

                // Assert
                var count = AsyncHelper.RunSync(() => connection.Hash.CountAsync(new BsonDocument()));
                Assert.Equal(0, count);
            }
        }


        [Fact, CleanDatabase]
        public void Execute_Processes_AggregatedCounterTable()
        {
            using (var connection = ConnectionUtils.CreateConnection())
            {
                // Arrange
                AsyncHelper.RunSync(() => connection.AggregatedCounter.InsertOneAsync(new AggregatedCounterDto
                {
                    Key = "key",
                    Value = 1,
                    ExpireAt = DateTime.UtcNow.AddMonths(-1)
                }));

                var manager = CreateManager();

                // Act
                manager.Execute(_token);

                // Assert
                Assert.Equal(0, AsyncHelper.RunSync(() => connection.Counter.Find(new BsonDocument()).CountAsync()));
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
            AsyncHelper.RunSync(() => connection.AggregatedCounter.InsertOneAsync(counter));

            var id = counter.Id;

            return id;
        }

        private static bool IsEntryExpired(HangfireDbContext connection, ObjectId entryId)
        {
            var count = AsyncHelper.RunSync(() => connection.AggregatedCounter.Find(Builders<AggregatedCounterDto>.Filter.Eq(_ => _.Id, entryId)).CountAsync());
            return count == 0;
        }

        private ExpirationManager CreateManager()
        {
            return new ExpirationManager(_storage);
        }
    }
#pragma warning restore 1591
}