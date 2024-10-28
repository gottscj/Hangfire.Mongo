using System;
using System.Threading;
using Hangfire.Mongo.Database;
using Hangfire.Mongo.DistributedLock;
using Hangfire.Mongo.Dto;
using Hangfire.Mongo.Tests.Utils;
using Hangfire.Storage;
using MongoDB.Bson;
using MongoDB.Driver;
using Xunit;

namespace Hangfire.Mongo.Tests
{
#pragma warning disable 1591

    [Collection("Database")]
    public class MongoDistributedLockFacts
    {
        private readonly HangfireDbContext _database;

        public MongoDistributedLockFacts(MongoIntegrationTestFixture fixture)
        {
            fixture.CleanDatabase();
            _database = fixture.CreateDbContext();
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenResourceIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(
                () => new MongoDistributedLock(null, TimeSpan.Zero, _database, new MongoStorageOptions()));

            Assert.Equal("resource", exception.ParamName);
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenConnectionIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(
                () => new MongoDistributedLock("resource1", TimeSpan.Zero, null,
                    new MongoStorageOptions()));

            Assert.Equal("dbContext", exception.ParamName);
        }

        [Fact]
        public void Ctor_SetLock_WhenResourceIsNotLocked()
        {
            var lock1 = new MongoDistributedLock("resource1", TimeSpan.Zero, _database, new MongoStorageOptions());
            using (lock1.AcquireLock())
            {
                var filter = new BsonDocument(nameof(DistributedLockDto.Resource), "resource1");
                var locksCount = _database.DistributedLock.Count(filter);
                Assert.Equal(1, locksCount);
            }
        }

        [Fact]
        public void Ctor_SetReleaseLock_WhenResourceIsNotLocked()
        {
            var lock1 = new MongoDistributedLock("resource1", TimeSpan.Zero, _database, new MongoStorageOptions());
            var filter = new BsonDocument(nameof(DistributedLockDto.Resource), "resource1");
            using (lock1.AcquireLock())
            {

                var locksCount = _database.DistributedLock.Count(filter);
                Assert.Equal(1, locksCount);
            }

            var locksCountAfter = _database.DistributedLock.Count(filter);
            Assert.Equal(0, locksCountAfter);
        }

        [Fact]
        public void Ctor_AcquireLockWithinSameThread_WhenResourceIsLocked()
        {
            var lock1 = new MongoDistributedLock("resource1", TimeSpan.Zero, _database, new MongoStorageOptions());
            var filter = new BsonDocument(nameof(DistributedLockDto.Resource), "resource1");
            using (lock1.AcquireLock())
            {

                var locksCount = _database.DistributedLock.Count(filter);
                Assert.Equal(1, locksCount);

                var lock2 = new MongoDistributedLock("resource1", TimeSpan.Zero, _database, new MongoStorageOptions());
                using (lock2.AcquireLock())
                {
                    locksCount = _database.DistributedLock.Count(filter);
                    Assert.Equal(1, locksCount);
                }
            }
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenResourceIsLocked()
        {
            var lock1 = new MongoDistributedLock("resource1", TimeSpan.Zero, _database, new MongoStorageOptions());
            var filter = new BsonDocument(nameof(DistributedLockDto.Resource), "resource1");
            using (lock1.AcquireLock())
            {
                var locksCount = _database.DistributedLock.Count(filter);
                Assert.Equal(1, locksCount);

                var t = new Thread(() =>
                {
                    Assert.Throws<DistributedLockTimeoutException>(() =>
                        {
                            var lock2 = new MongoDistributedLock("resource1", TimeSpan.Zero, _database,
                                new MongoStorageOptions());
                            lock2.AcquireLock();
                        }
                        );
                });
                t.Start();
                Assert.True(t.Join(5000), "Thread is hanging unexpected");
            }
        }

        [Fact]
        public void Ctor_WaitForLock_SignaledAtLockRelease()
        {
            var t = new Thread(() =>
            {
                var lock1 = new MongoDistributedLock("resource1", TimeSpan.Zero, _database, new MongoStorageOptions());
                using (lock1.AcquireLock())
                {
                    Thread.Sleep(TimeSpan.FromSeconds(3));
                }
            });
            t.Start();

            // Wait just a bit to make sure the above lock is acquired
            Thread.Sleep(TimeSpan.FromSeconds(1));

            // Record when we try to acquire the lock
            var startTime = DateTime.UtcNow;
            var lock2 = new MongoDistributedLock("resource1", TimeSpan.FromSeconds(10), _database,
                new MongoStorageOptions());
            using (lock2.AcquireLock())
            {
                Assert.InRange(DateTime.UtcNow - startTime, TimeSpan.Zero, TimeSpan.FromSeconds(5));
            }
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenOptionsIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(() =>
            {
                var lock1 = new MongoDistributedLock("resource1", TimeSpan.Zero, _database, null);
                lock1.AcquireLock();
            });

            Assert.Equal("storageOptions", exception.ParamName);
        }

        [Fact]
        public void Ctor_SetLockExpireAtWorks_WhenResourceIsNotLocked()
        {
            var lock1 = new MongoDistributedLock("resource1", TimeSpan.Zero, _database,
                new MongoStorageOptions() {DistributedLockLifetime = TimeSpan.FromSeconds(3)});
            var filter = new BsonDocument
            {
                [nameof(DistributedLockDto.Resource)] = "resource1"
            };
            using (lock1.AcquireLock())
            {
                var initialExpireAt = DateTime.UtcNow;
                Thread.Sleep(TimeSpan.FromSeconds(5));

                var lockEntry = _database.DistributedLock.Find(filter).FirstOrDefault();
                Assert.NotNull(lockEntry);
                Assert.True(new DistributedLockDto(lockEntry).ExpireAt > initialExpireAt);
            }
        }

        [Fact]
        public void Ctor_AcquireLock_WhenLockExpired()
        {
            // simulate situation when lock was not disposed correctly (app crash) and there is no heartbeats to prolong ExpireAt value
            var initialExpireAt = DateTime.UtcNow.AddSeconds(3);
            _database.DistributedLock
                .InsertOne(new DistributedLockDto {ExpireAt = initialExpireAt, Resource = "resource1" }.Serialize());

            var lock1 = new MongoDistributedLock("resource1", TimeSpan.FromSeconds(5), _database,
                new MongoStorageOptions());
            var filter = new BsonDocument
            {
                [nameof(DistributedLockDto.Resource)] = "resource1"
            };
            using (lock1.AcquireLock())
            {
                var lockEntry = new DistributedLockDto(_database.DistributedLock.Find(filter).Single());

                Assert.True(lockEntry.ExpireAt > initialExpireAt);
            }
        }
    }
#pragma warning restore 1591
}