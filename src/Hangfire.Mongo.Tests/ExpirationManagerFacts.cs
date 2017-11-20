using System;
using System.Collections.Generic;
using System.Threading;
using Hangfire.Mongo.Database;
using Hangfire.Mongo.Dto;
using Hangfire.Mongo.PersistentJobQueue;
using Hangfire.Mongo.Tests.Utils;
using MongoDB.Bson;
using Xunit;

namespace Hangfire.Mongo.Tests
{
    [Collection("Database")]
    public class ExpirationManagerFacts
    {
        private readonly MongoStorage _storage;

        private readonly CancellationToken _token;
        private static PersistentJobQueueProviderCollection _queueProviders;

        public ExpirationManagerFacts()
        {
            _storage = ConnectionUtils.CreateStorage();
            _queueProviders = _storage.QueueProviders;

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
            ConnectionUtils.UseConnection(connection =>
            {
                // ARRANGE
                CreateExpirationEntries(connection, DateTime.UtcNow.AddMonths(-1));
                var manager = CreateManager();

                // ACT
                manager.Execute(_token);

                // ASSERT
                Assert.True(IsEntryExpired(connection));
            });
        }

        [Fact, CleanDatabase]
        public void Execute_DoesNotRemoveEntries_WithNoExpirationTimeSet()
        {
            ConnectionUtils.UseConnection(connection =>
            {
                // ARRANGE
                CreateExpirationEntries(connection, null);
                var manager = CreateManager();

                // ACT
                manager.Execute(_token);

                // ASSERT
                Assert.False(IsEntryExpired(connection));
            });
        }

        [Fact, CleanDatabase]
        public void Execute_DoesNotRemoveEntries_WithFreshExpirationTime()
        {
            ConnectionUtils.UseConnection(connection =>
            {
                // ARRANGE
                CreateExpirationEntries(connection, DateTime.Now.AddMonths(1));
                var manager = CreateManager();

                // ACT
                manager.Execute(_token);

                // ASSERT
                Assert.False(IsEntryExpired(connection));
            });
        }

        [Fact, CleanDatabase]
        public void Execute_Processes_CounterTable()
        {
            ConnectionUtils.UseConnection(connection =>
            {
                // Arrange
                connection.StateData.InsertOne(new CounterDto
                {
                    Id = ObjectId.GenerateNewId(),
                    Key = "key",
                    Value = 1L,
                    ExpireAt = DateTime.UtcNow.AddMonths(-1)
                });

                var manager = CreateManager();

                // Act
                manager.Execute(_token);

                // Assert
                Assert.Equal(0, connection.StateData.OfType<CounterDto>().Count(new BsonDocument()));
            });
        }

        [Fact, CleanDatabase]
        public void Execute_Processes_JobTable()
        {
            ConnectionUtils.UseConnection(connection =>
            {
                // Arrange
                connection.Job.InsertOne(new JobDto
                {
                    Id = ObjectId.GenerateNewId(),
                    InvocationData = "",
                    Arguments = "",
                    CreatedAt = DateTime.UtcNow,
                    ExpireAt = DateTime.UtcNow.AddMonths(-1),
                });

                var manager = CreateManager();

                // Act
                manager.Execute(_token);

                // Assert
                Assert.Equal(0, connection.Job.Count(new BsonDocument()));
            });
        }

        [Fact, CleanDatabase]
        public void Execute_Processes_ListTable()
        {
            ConnectionUtils.UseConnection(connection =>
            {
                // Arrange
                connection.StateData.InsertOne(new ListDto
                {
                    Id = ObjectId.GenerateNewId(),
                    Key = "key",
                    ExpireAt = DateTime.UtcNow.AddMonths(-1)
                });

                var manager = CreateManager();

                // Act
                manager.Execute(_token);

                // Assert
                Assert.Equal(0, connection.StateData.OfType<ListDto>().Count(new BsonDocument()));
            });
        }

        [Fact, CleanDatabase]
        public void Execute_Processes_SetTable()
        {
            ConnectionUtils.UseConnection(connection =>
            {
                // Arrange
                connection.StateData.InsertOne(new SetDto
                {
                    Id = ObjectId.GenerateNewId(),
                    Key = "key",
                    Score = 0,
                    Value = "",
                    ExpireAt = DateTime.UtcNow.AddMonths(-1)
                });

                var manager = CreateManager();

                // Act
                manager.Execute(_token);

                // Assert
                Assert.Equal(0, connection.StateData.OfType<SetDto>().Count(new BsonDocument()));
            });
        }

        [Fact, CleanDatabase]
        public void Execute_Processes_HashTable()
        {
            ConnectionUtils.UseConnection(connection =>
            {
                // Arrange
                connection.StateData.InsertOne(new HashDto
                {
                    Id = ObjectId.GenerateNewId(),
                    Key = "key",
                    Field = "field",
                    Value = "",
                    ExpireAt = DateTime.UtcNow.AddMonths(-1)
                });

                var manager = CreateManager();

                // Act
                manager.Execute(_token);

                // Assert
                Assert.Equal(0, connection.StateData.OfType<HashDto>().Count(new BsonDocument()));
            });
        }

        [Fact, CleanDatabase]
        public void Execute_Processes_AggregatedCounterTable()
        {
            ConnectionUtils.UseConnection(connection =>
            {
                // Arrange
                connection.StateData.InsertOne(new AggregatedCounterDto
                {
                    Key = "key",
                    Value = 1,
                    ExpireAt = DateTime.UtcNow.AddMonths(-1)
                });

                var manager = CreateManager();

                // Act
                manager.Execute(_token);

                // Assert
                Assert.Equal(0, connection.StateData.OfType<CounterDto>().Count(new BsonDocument()));
            });
        }



        private static void CreateExpirationEntries(HangfireDbContext database, DateTime? expireAt)
        {
            Commit(database, x => x.AddToSet("my-key", "my-value"));
            Commit(database, x => x.AddToSet("my-key", "my-value1"));
            Commit(database, x => x.SetRangeInHash("my-hash-key", new[] { new KeyValuePair<string, string>("key", "value"), new KeyValuePair<string, string>("key1", "value1") }));
            Commit(database, x => x.AddRangeToSet("my-key", new[] { "my-value", "my-value1" }));

            if (expireAt.HasValue)
            {
                var expireIn = expireAt.Value - DateTime.Now;
                Commit(database, x => x.ExpireHash("my-hash-key", expireIn));
                Commit(database, x => x.ExpireSet("my-key", expireIn));
            }
        }

        private static bool IsEntryExpired(HangfireDbContext database)
        {
            var count = database
                .StateData
                .OfType<ExpiringKeyValueDto>()
                .Count(new BsonDocument());

            return count == 0;
        }

        private ExpirationManager CreateManager()
        {
            return new ExpirationManager(_storage);
        }

        private static void Commit(HangfireDbContext database, Action<MongoWriteOnlyTransaction> action)
        {
            using (MongoWriteOnlyTransaction transaction = new MongoWriteOnlyTransaction(database, _queueProviders, new MongoStorageOptions()))
            {
                action(transaction);
                transaction.Commit();
            }
        }
    }
}