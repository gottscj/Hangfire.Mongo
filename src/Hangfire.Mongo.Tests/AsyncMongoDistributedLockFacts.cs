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
    public class AsyncMongoDistributedLockFacts
    {
        private const string Resource = "resource1";
        private static readonly TimeSpan LockLifetime = TimeSpan.FromSeconds(1);

        private readonly MongoStorageOptions _options = new MongoStorageOptions
        {
            DistributedLockLifetime = LockLifetime
        };
        private readonly BsonDocument _filter = new BsonDocument(nameof(DistributedLockDto.Resource), Resource);
        private readonly HangfireDbContext _database;

        public AsyncMongoDistributedLockFacts(MongoIntegrationTestFixture fixture)
        {
            fixture.CleanDatabase();
            _database = fixture.CreateDbContext();
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenResourceIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(
                () => new AsyncMongoDistributedLock(null, TimeSpan.Zero, _database, _options));

            Assert.Equal("resource", exception.ParamName);
        }

        [Fact]
        public void AcquireLock_StoresOwnerToken_AndDisposeReleasesLock()
        {
            using (CreateLock(TimeSpan.Zero).AcquireLock())
            {
                var lockEntry = GetLock();
                Assert.False(string.IsNullOrEmpty(lockEntry.OwnerToken), "Expected an owner token");
            }

            Assert.Equal(0, _database.DistributedLock.CountDocuments(_filter));
        }

        [Fact]
        public void AcquireLock_UsesDistinctOwnerTokens_ForSubsequentLocks()
        {
            string firstOwner;
            using (CreateLock(TimeSpan.Zero).AcquireLock())
            {
                firstOwner = GetLock().OwnerToken;
            }

            using (CreateLock(TimeSpan.Zero).AcquireLock())
            {
                Assert.NotEqual(firstOwner, GetLock().OwnerToken);
            }
        }

        [Fact]
        public void AcquireLock_IsReentrant_WithinSameThread()
        {
            using (CreateLock(TimeSpan.Zero).AcquireLock())
            {
                var owner = GetLock().OwnerToken;
                using (CreateLock(TimeSpan.Zero).AcquireLock())
                {
                    Assert.Equal(1, _database.DistributedLock.CountDocuments(_filter));
                }

                Assert.Equal(owner, GetLock().OwnerToken);
            }

            Assert.Equal(0, _database.DistributedLock.CountDocuments(_filter));
        }

        [Fact]
        public void Dispose_ReleasesLock_WhenReentrantLocksAreDisposedOutOfOrder()
        {
            var outer = CreateLock(TimeSpan.Zero).AcquireLock();
            var inner = CreateLock(TimeSpan.Zero).AcquireLock();

            outer.Dispose();
            Assert.Equal(1, _database.DistributedLock.CountDocuments(_filter));

            inner.Dispose();
            Assert.Equal(0, _database.DistributedLock.CountDocuments(_filter));
        }

        [Fact]
        public void AcquireLock_ThrowsAnException_WhenResourceIsLockedByAnotherThread()
        {
            using (CreateLock(TimeSpan.Zero).AcquireLock())
            {
                Exception exception = null;
                var t = new Thread(() =>
                {
                    exception = Record.Exception(() => CreateLock(TimeSpan.Zero).AcquireLock());
                });
                t.Start();

                Assert.True(t.Join(5000), "Thread is hanging unexpected");
                Assert.IsType<DistributedLockTimeoutException>(exception);
            }
        }

        [Fact]
        public void AcquireLock_WaitsForLock_UntilReleased()
        {
            var acquired = new ManualResetEventSlim();
            var t = new Thread(() =>
            {
                using (CreateLock(TimeSpan.Zero).AcquireLock())
                {
                    acquired.Set();
                    Thread.Sleep(TimeSpan.FromSeconds(2));
                }
            });
            t.Start();
            Assert.True(acquired.Wait(TimeSpan.FromSeconds(5)), "Expected first lock to be acquired");

            var startTime = DateTime.UtcNow;
            using (CreateLock(TimeSpan.FromSeconds(10)).AcquireLock())
            {
                Assert.InRange(DateTime.UtcNow - startTime, TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(5));
            }

            Assert.True(t.Join(5000));
        }

        [Fact]
        public void AcquireLock_TakesOverExpiredLock_WithoutOwnerToken()
        {
            // a lock left behind by a crashed MongoDistributedLock
            _database.DistributedLock.InsertOne(new DistributedLockDto
            {
                Id = ObjectId.GenerateNewId(),
                Resource = Resource,
                ExpireAt = DateTime.UtcNow.AddSeconds(1)
            }.Serialize());

            using (CreateLock(TimeSpan.FromSeconds(5)).AcquireLock())
            {
                Assert.False(string.IsNullOrEmpty(GetLock().OwnerToken), "Expected lock to be taken over");
            }
        }

        [Fact]
        public void Heartbeat_ExtendsLock_WhileHeld()
        {
            using (CreateLock(TimeSpan.Zero).AcquireLock())
            {
                var initialExpireAt = GetLock().ExpireAt;

                Assert.True(Wait.Until(() => GetLock().ExpireAt > initialExpireAt), "Expected heartbeat to extend the lock");
                // lock outlives its lifetime while held
                Thread.Sleep(LockLifetime * 2);
                Assert.Equal(1, _database.DistributedLock.CountDocuments(_filter));
                Assert.True(GetLock().ExpireAt > DateTime.UtcNow);
            }
        }

        [Fact]
        public void Heartbeat_LeavesLock_WhenOwnershipChanged()
        {
            using (CreateLock(TimeSpan.Zero).AcquireLock())
            {
                var otherOwner = Guid.NewGuid().ToString("N");
                var otherExpireAt = DateTime.UtcNow.AddMinutes(5);
                SetOwner(otherOwner, otherExpireAt);

                Thread.Sleep(LockLifetime * 2);

                var lockEntry = GetLock();
                Assert.Equal(otherOwner, lockEntry.OwnerToken);
                Assert.Equal(otherExpireAt, lockEntry.ExpireAt, TimeSpan.FromSeconds(1));
            }
        }

        [Fact]
        public void Dispose_LeavesLock_WhenOwnershipChanged()
        {
            var otherOwner = Guid.NewGuid().ToString("N");
            using (CreateLock(TimeSpan.Zero).AcquireLock())
            {
                SetOwner(otherOwner, DateTime.UtcNow.AddMinutes(5));
            }

            Assert.Equal(otherOwner, GetLock().OwnerToken);
        }

        [Fact]
        public void Dispose_StopsHeartbeat()
        {
            var distributedLock = CreateLock(TimeSpan.Zero);
            distributedLock.AcquireLock();
            var owner = GetLock().OwnerToken;
            distributedLock.Dispose();

            // put the lock back as if it was still held, the stopped heartbeat must not extend it
            var expireAt = DateTime.UtcNow.AddMinutes(-1);
            _database.DistributedLock.InsertOne(new BsonDocument
            {
                ["_id"] = ObjectId.GenerateNewId(),
                [nameof(DistributedLockDto.Resource)] = Resource,
                [nameof(DistributedLockDto.OwnerToken)] = owner,
                [nameof(DistributedLockDto.ExpireAt)] = expireAt
            });
            Thread.Sleep(LockLifetime * 2);

            Assert.Equal(expireAt, GetLock().ExpireAt, TimeSpan.FromSeconds(1));
        }

        private AsyncMongoDistributedLock CreateLock(TimeSpan timeout)
        {
            return new AsyncMongoDistributedLock(Resource, timeout, _database, _options);
        }

        private DistributedLockDto GetLock()
        {
            return new DistributedLockDto(_database.DistributedLock.Find(_filter).Single());
        }

        private void SetOwner(string ownerToken, DateTime expireAt)
        {
            _database.DistributedLock.UpdateOne(_filter, new BsonDocument("$set", new BsonDocument
            {
                [nameof(DistributedLockDto.OwnerToken)] = ownerToken,
                [nameof(DistributedLockDto.ExpireAt)] = expireAt
            }));
        }
    }
#pragma warning restore 1591
}
