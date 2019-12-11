using System;
using System.Threading;
using Hangfire.Mongo.Database;
using Hangfire.Mongo.DistributedLock;
using Hangfire.Mongo.Dto;
using Hangfire.Mongo.Tests.Utils;
using Hangfire.Storage;
using MongoDB.Driver;
using Xunit;

namespace Hangfire.Mongo.Tests
{
#pragma warning disable 1591

    [Collection("Database")]
    public class MongoDistributedLockFacts
    {
        private readonly IDistributedLockMutex _distributedLockMutex = new DistributedLockMutex();
        private readonly HangfireDbContext _database = ConnectionUtils.CreateDbContext();
        [Fact]
        public void Ctor_ThrowsAnException_WhenResourceIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(
                () => new MongoDistributedLock(null, TimeSpan.Zero, _database, new MongoStorageOptions(),
                    _distributedLockMutex));

            Assert.Equal("resource", exception.ParamName);
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenConnectionIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(
                () => new MongoDistributedLock("resource1", TimeSpan.Zero, null,
                    new MongoStorageOptions(), _distributedLockMutex));

            Assert.Equal("locks", exception.ParamName);
        }

        [Fact, CleanDatabase]
        public void Ctor_SetLock_WhenResourceIsNotLocked()
        {
            using (
                new MongoDistributedLock("resource1", TimeSpan.Zero, _database, new MongoStorageOptions(),
                    _distributedLockMutex))
            {
                var locksCount =
                    _database.DistributedLock.Count(Builders<DistributedLockDto>.Filter.Eq(_ => _.Resource,
                        "resource1"));
                Assert.Equal(1, locksCount);
            }
        }

        [Fact, CleanDatabase]
        public void Ctor_SetReleaseLock_WhenResourceIsNotLocked()
        {
            using (new MongoDistributedLock("resource1", TimeSpan.Zero, _database, new MongoStorageOptions(),
                _distributedLockMutex))
            {
                var locksCount =
                    _database.DistributedLock.Count(
                        Builders<DistributedLockDto>.Filter.Eq(_ => _.Resource, "resource1"));
                Assert.Equal(1, locksCount);
            }

            var locksCountAfter =
                _database.DistributedLock.Count(
                    Builders<DistributedLockDto>.Filter.Eq(_ => _.Resource, "resource1"));
            Assert.Equal(0, locksCountAfter);
        }

        [Fact, CleanDatabase]
        public void Ctor_AcquireLockWithinSameThread_WhenResourceIsLocked()
        {
            using (new MongoDistributedLock("resource1", TimeSpan.Zero, _database, new MongoStorageOptions(),
                _distributedLockMutex))
            {
                var locksCount =
                    _database.DistributedLock.Count(
                        Builders<DistributedLockDto>.Filter.Eq(_ => _.Resource, "resource1"));
                Assert.Equal(1, locksCount);

                using (new MongoDistributedLock("resource1", TimeSpan.Zero, _database, new MongoStorageOptions(),
                    _distributedLockMutex))
                {
                    locksCount =
                        _database.DistributedLock.Count(
                            Builders<DistributedLockDto>.Filter.Eq(_ => _.Resource, "resource1"));
                    Assert.Equal(1, locksCount);
                }
            }
        }

        [Fact, CleanDatabase]
        public void Ctor_ThrowsAnException_WhenResourceIsLocked()
        {
            using (new MongoDistributedLock("resource1", TimeSpan.Zero, _database, new MongoStorageOptions(),
                _distributedLockMutex))
            {
                var locksCount =
                    _database.DistributedLock.Count(
                        Builders<DistributedLockDto>.Filter.Eq(_ => _.Resource, "resource1"));
                Assert.Equal(1, locksCount);

                var t = new Thread(() =>
                {
                    Assert.Throws<DistributedLockTimeoutException>(() =>
                        new MongoDistributedLock("resource1", TimeSpan.Zero, _database, new MongoStorageOptions(),
                            _distributedLockMutex));
                });
                t.Start();
                Assert.True(t.Join(5000), "Thread is hanging unexpected");
            }
        }

        [Fact, CleanDatabase]
        public void Ctor_WaitForLock_SignaledAtLockRelease()
        {
            var t = new Thread(() =>
            {
                using (new MongoDistributedLock("resource1", TimeSpan.Zero, _database, new MongoStorageOptions(),
                    _distributedLockMutex))
                {
                    Thread.Sleep(TimeSpan.FromSeconds(3));
                }
            });
            t.Start();

            // Wait just a bit to make sure the above lock is acuired
            Thread.Sleep(TimeSpan.FromSeconds(1));

            // Record when we try to aquire the lock
            var startTime = DateTime.UtcNow;
            using (new MongoDistributedLock("resource1", TimeSpan.FromSeconds(10), _database,
                new MongoStorageOptions(), _distributedLockMutex))
            {
                Assert.InRange(DateTime.UtcNow - startTime, TimeSpan.Zero, TimeSpan.FromSeconds(5));
            }
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenOptionsIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(() =>
                new MongoDistributedLock("resource1", TimeSpan.Zero, _database, null, _distributedLockMutex));

            Assert.Equal("storageOptions", exception.ParamName);
        }

        [Fact, CleanDatabase]
        public void Ctor_SetLockExpireAtWorks_WhenResourceIsNotLocked()
        {
            using (new MongoDistributedLock("resource1", TimeSpan.Zero, _database,
                new MongoStorageOptions() {DistributedLockLifetime = TimeSpan.FromSeconds(3)},
                _distributedLockMutex))
            {
                DateTime initialExpireAt = DateTime.UtcNow;
                Thread.Sleep(TimeSpan.FromSeconds(5));

                DistributedLockDto lockEntry = _database.DistributedLock
                    .Find(Builders<DistributedLockDto>.Filter.Eq(_ => _.Resource, "resource1")).FirstOrDefault();
                Assert.NotNull(lockEntry);
                Assert.True(lockEntry.ExpireAt > initialExpireAt);
            }
        }

        [Fact, CleanDatabase]
        public void Ctor_AcquireLock_WhenLockExpired()
        {
            // simulate situation when lock was not disposed correctly (app crash) and there is no heartbeats to prolong ExpireAt value
            var initialExpireAt = DateTime.UtcNow.AddSeconds(3);
            _database.DistributedLock.InsertOne(new DistributedLockDto {ExpireAt = initialExpireAt, Resource = "resource1" });

            using (new MongoDistributedLock("resource1", TimeSpan.FromSeconds(5), _database, new MongoStorageOptions(),
                _distributedLockMutex))
            {
                var lockEntry = _database.DistributedLock
                    .Find(Builders<DistributedLockDto>.Filter.Eq(_ => _.Resource, "resource1")).Single();
                Assert.True(lockEntry.ExpireAt > initialExpireAt);
            }
        }
    }
#pragma warning restore 1591
}