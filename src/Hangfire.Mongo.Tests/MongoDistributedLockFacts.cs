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

    [Collection("DistributedLock")]
    public class MongoDistributedLockFacts
    {

        [Fact]
        public void Ctor_ThrowsAnException_WhenResourceIsNull()
        {
            UseConnection(database =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => new MongoDistributedLock(null, TimeSpan.Zero, database, new MongoStorageOptions()));

                Assert.Equal("resource", exception.ParamName);
            });
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenResourceIsEmpty()
        {
            UseConnection(database =>
            {
                var exception = Assert.Throws<ArgumentException>(
                    () => new MongoDistributedLock(string.Empty, TimeSpan.Zero, database, new MongoStorageOptions()));

                Assert.Equal("resource", exception.ParamName);
            });
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenTimeoutIsToLong()
        {
            UseConnection(database =>
            {
                // Make a unique resource name so we do not conflict with others
                var resource = Guid.NewGuid().ToString();
                var exception = Assert.Throws<ArgumentException>(
                    () => new MongoDistributedLock(resource, TimeSpan.MaxValue, database, new MongoStorageOptions()));

                Assert.Equal("timeout", exception.ParamName);
            });
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenConnectionIsNull()
        {
            UseConnection(database =>
            {
                // Make a unique resource name so we do not conflict with others
                var resource = Guid.NewGuid().ToString();
                var exception = Assert.Throws<ArgumentNullException>(
                    () => new MongoDistributedLock(resource, TimeSpan.Zero, null, new MongoStorageOptions()));

                Assert.Equal("database", exception.ParamName);
            });
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenStorageOptionsIsNull()
        {
            UseConnection(database =>
            {
                // Make a unique resource name so we do not conflict with others
                var resource = Guid.NewGuid().ToString();
                var exception = Assert.Throws<ArgumentNullException>(
                    () => new MongoDistributedLock(resource, TimeSpan.Zero, database, null));

                Assert.Equal("storageOptions", exception.ParamName);
            });
        }

        [Fact]
        public void Ctor_SetLock_WhenResourceIsNotLocked()
        {
            UseConnection(database =>
            {
                // Make a unique resource name so we do not conflict with others
                var resource = Guid.NewGuid().ToString();
                using (new MongoDistributedLock(resource, TimeSpan.Zero, database, new MongoStorageOptions()))
                {
                    var filter = Builders<DistributedLockDto>.Filter.Eq(_ => _.Resource, resource);
                    var locksCount = database.DistributedLock.Count(filter);
                    Assert.Equal(1, locksCount);
                }
            });
        }

        [Fact]
        public void Ctor_SetReleaseLock_WhenResourceIsNotLocked()
        {
            UseConnection(database =>
            {
                // Make a unique resource name so we do not conflict with others
                var resource = Guid.NewGuid().ToString();
                var filter = Builders<DistributedLockDto>.Filter.Eq(_ => _.Resource, resource);
                using (new MongoDistributedLock(resource, TimeSpan.Zero, database, new MongoStorageOptions()))
                {
                    var locksCount = database.DistributedLock.Count(filter);
                    Assert.Equal(1, locksCount);
                }

                var locksCountAfter = database.DistributedLock.Count(filter);
                Assert.Equal(0, locksCountAfter);
            });
        }

        [Fact]
        public void Ctor_AcquireLockWithinSameThread_WhenResourceIsLocked()
        {
            UseConnection(database =>
            {
                // Make a unique resource name so we do not conflict with others
                var resource = Guid.NewGuid().ToString();
                using (new MongoDistributedLock(resource, TimeSpan.Zero, database, new MongoStorageOptions()))
                {
                    var filter = Builders<DistributedLockDto>.Filter.Eq(_ => _.Resource, resource);
                    var locksCount = database.DistributedLock.Count(filter);
                    Assert.Equal(1, locksCount);

                    using (new MongoDistributedLock(resource, TimeSpan.Zero, database, new MongoStorageOptions()))
                    {
                        locksCount = database.DistributedLock.Count(filter);
                        Assert.Equal(1, locksCount);
                    }
                }
            });
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenResourceIsLocked()
        {
            UseConnection(database =>
            {
                // Make a unique resource name so we do not conflict with others
                var resource = Guid.NewGuid().ToString();
                using (new MongoDistributedLock(resource, TimeSpan.Zero, database, new MongoStorageOptions()))
                {
                    var filter = Builders<DistributedLockDto>.Filter.Eq(_ => _.Resource, resource);
                    var locksCount = database.DistributedLock.Count(filter);
                    Assert.Equal(1, locksCount);

                    var t = new Thread(() =>
                    {
                        Assert.Throws<DistributedLockTimeoutException>(() =>
                                new MongoDistributedLock(resource, TimeSpan.Zero, database, new MongoStorageOptions()));
                    });
                    t.Start();
                    Assert.True(t.Join(5000), "Thread is hanging unexpected");
                }
            });
        }

        [Fact, MongoSignal]
        public void Ctor_WaitForLock_SignaledAtLockRelease()
        {
            UseConnection(database =>
            {
                // Make a unique resource name so we do not conflict with others
                var resource = Guid.NewGuid().ToString();
                var waitHandle = new AutoResetEvent(false);
                var options = new MongoStorageOptions();
                var t = new Thread(() =>
                {
                    using (new MongoDistributedLock(resource, TimeSpan.Zero, database, options))
                    {
                        waitHandle.Set();
                        Thread.Sleep(TimeSpan.FromSeconds(3));
                    }
                });
                t.Start();

                // Wait for the lock to be acuired in the thread
                // - but bail out at timeout so we do not hang forever.
                Assert.True(waitHandle.WaitOne(TimeSpan.FromSeconds(10)), "Timed out waiting for the lock to be aqquired");

                var startTime = DateTime.Now; // Record when we try to acquire the lock

                // Give 5 secs to acquire the lock - it should be released after 3 secs
                using (new MongoDistributedLock(resource, TimeSpan.FromSeconds(5), database, options))
                {
                    Assert.InRange(DateTime.Now - startTime, TimeSpan.Zero, TimeSpan.FromSeconds(5));
                }
            });
        }

        [Fact]
        public void Ctor_SetLockExpireAtWorks_WhenResourceIsNotLocked()
        {
            UseConnection(database =>
            {
                // Make a unique resource name so we do not conflict with others
                var resource = Guid.NewGuid().ToString();
                var storageOption = new MongoStorageOptions
                {
                    DistributedLockLifetime = TimeSpan.FromSeconds(3)
                };
                using (new MongoDistributedLock(resource, TimeSpan.Zero, database, storageOption))
                {
                    DateTime initialExpireAt = DateTime.UtcNow;
                    Thread.Sleep(TimeSpan.FromSeconds(5));

                    var filter = Builders<DistributedLockDto>.Filter.Eq(_ => _.Resource, resource);
                    DistributedLockDto lockEntry = database.DistributedLock.Find(filter).FirstOrDefault();
                    Assert.NotNull(lockEntry);
                    Assert.True(lockEntry.ExpireAt > initialExpireAt);
                }
            });
        }

        private static void UseConnection(Action<HangfireDbContext> action)
        {
            using (var database = ConnectionUtils.CreateConnection())
            {
                action(database);
            }
        }

    }
}