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

    [Collection("Database")]
    public class MongoDistributedLockFacts
    {

        [Fact]
        public void Ctor_ThrowsAnException_WhenResourceIsNull()
        {
            ConnectionUtils.UseConnection(database =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => new MongoDistributedLock(null, TimeSpan.Zero, database, new MongoStorageOptions()));

                Assert.Equal("resource", exception.ParamName);
            });
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenResourceIsEmpty()
        {
            ConnectionUtils.UseConnection(database =>
            {
                var exception = Assert.Throws<ArgumentException>(
                    () => new MongoDistributedLock(string.Empty, TimeSpan.Zero, database, new MongoStorageOptions()));

                Assert.Equal("resource", exception.ParamName);
            });
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenTimeoutIsToLong()
        {
            ConnectionUtils.UseConnection(database =>
            {
                var exception = Assert.Throws<ArgumentException>(
                    () => new MongoDistributedLock("resource1", TimeSpan.MaxValue, database, new MongoStorageOptions()));

                Assert.Equal("timeout", exception.ParamName);
            });
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenConnectionIsNull()
        {
            ConnectionUtils.UseConnection(database =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => new MongoDistributedLock("resource1", TimeSpan.Zero, null, new MongoStorageOptions()));

                Assert.Equal("database", exception.ParamName);
            });
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenStorageOptionsIsNull()
        {
            ConnectionUtils.UseConnection(database =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => new MongoDistributedLock("resource1", TimeSpan.Zero, database, null));

                Assert.Equal("storageOptions", exception.ParamName);
            });
        }

        [Fact, CleanDatabase]
        public void Ctor_SetLock_WhenResourceIsNotLocked()
        {
            ConnectionUtils.UseConnection(database =>
            {
                using (new MongoDistributedLock("resource1", TimeSpan.Zero, database, new MongoStorageOptions()))
                {
                    var filter = Builders<DistributedLockDto>.Filter.Eq(_ => _.Resource, "resource1");
                    var locksCount = database.DistributedLock.Count(filter);
                    Assert.Equal(1, locksCount);
                }
            });
        }

        [Fact, CleanDatabase]
        public void Ctor_SetReleaseLock_WhenResourceIsNotLocked()
        {
            ConnectionUtils.UseConnection(database =>
            {
                var filter = Builders<DistributedLockDto>.Filter.Eq(_ => _.Resource, "resource1");
                using (new MongoDistributedLock("resource1", TimeSpan.Zero, database, new MongoStorageOptions()))
                {
                    var locksCount = database.DistributedLock.Count(filter);
                    Assert.Equal(1, locksCount);
                }

                var locksCountAfter = database.DistributedLock.Count(filter);
                Assert.Equal(0, locksCountAfter);
            });
        }

        [Fact, CleanDatabase]
        public void Ctor_AcquireLockWithinSameThread_WhenResourceIsLocked()
        {
            ConnectionUtils.UseConnection(database =>
            {
                using (new MongoDistributedLock("resource1", TimeSpan.Zero, database, new MongoStorageOptions()))
                {
                    var filter = Builders<DistributedLockDto>.Filter.Eq(_ => _.Resource, "resource1");
                    var locksCount = database.DistributedLock.Count(filter);
                    Assert.Equal(1, locksCount);

                    using (new MongoDistributedLock("resource1", TimeSpan.Zero, database, new MongoStorageOptions()))
                    {
                        locksCount = database.DistributedLock.Count(filter);
                        Assert.Equal(1, locksCount);
                    }
                }
            });
        }

        [Fact, CleanDatabase]
        public void Ctor_ThrowsAnException_WhenResourceIsLocked()
        {
            ConnectionUtils.UseConnection(database =>
            {
                using (new MongoDistributedLock("resource1", TimeSpan.Zero, database, new MongoStorageOptions()))
                {
                    var filter = Builders<DistributedLockDto>.Filter.Eq(_ => _.Resource, "resource1");
                    var locksCount = database.DistributedLock.Count(filter);
                    Assert.Equal(1, locksCount);

                    var t = new Thread(() =>
                    {
                        Assert.Throws<DistributedLockTimeoutException>(() =>
                                new MongoDistributedLock("resource1", TimeSpan.Zero, database, new MongoStorageOptions()));
                    });
                    t.Start();
                    Assert.True(t.Join(5000), "Thread is hanging unexpected");
                }
            });
        }

        [Fact, CleanDatabase, MongoSignal]
        public void Ctor_WaitForLock_SignaledAtLockRelease()
        {
            ConnectionUtils.UseConnection(database =>
            {
                var t = new Thread(() =>
                {
                    using (new MongoDistributedLock("resource1", TimeSpan.Zero, database, new MongoStorageOptions()))
                    {
                        Thread.Sleep(TimeSpan.FromSeconds(3));
                    }
                });
                t.Start();

                // Wait just a bit to make sure the above lock is acuired
                Thread.Sleep(TimeSpan.FromSeconds(1));

                // Record when we try to aquire the lock
                var startTime = DateTime.Now;
                using (new MongoDistributedLock("resource1", TimeSpan.FromSeconds(10), database, new MongoStorageOptions()))
                {
                    Assert.InRange(DateTime.Now - startTime, TimeSpan.Zero, TimeSpan.FromSeconds(5));
                }
            });
        }

        [Fact, CleanDatabase]
        public void Ctor_SetLockExpireAtWorks_WhenResourceIsNotLocked()
        {
            ConnectionUtils.UseConnection(database =>
            {
                var storageOption = new MongoStorageOptions
                {
                    DistributedLockLifetime = TimeSpan.FromSeconds(3)
                };
                using (new MongoDistributedLock("resource1", TimeSpan.Zero, database, storageOption))
                {
                    DateTime initialExpireAt = DateTime.UtcNow;
                    Thread.Sleep(TimeSpan.FromSeconds(5));

                    var filter = Builders<DistributedLockDto>.Filter.Eq(_ => _.Resource, "resource1");
                    DistributedLockDto lockEntry = database.DistributedLock.Find(filter).FirstOrDefault();
                    Assert.NotNull(lockEntry);
                    Assert.True(lockEntry.ExpireAt > initialExpireAt);
                }
            });
        }

    }
}