using System;
using System.Threading;
using Hangfire.Mongo.Database;
using Hangfire.Mongo.Dto;
using Hangfire.Mongo.Migration;
using Hangfire.Mongo.Tests.Utils;
using MongoDB.Bson;
using MongoDB.Driver;
using Xunit;

namespace Hangfire.Mongo.Tests
{
#pragma warning disable 1591

    [Collection("Database")]
    public class MongoMigrationLockFacts
    {
        private readonly HangfireDbContext _database;
        private readonly MongoStorageOptions _options;
        private readonly IMongoCollection<BsonDocument> _locks;
        public MongoMigrationLockFacts(MongoIntegrationTestFixture fixture)
        {
            fixture.CleanDatabase();
            _database = fixture.CreateDbContext();
            _options = new MongoStorageOptions();
            _locks = _database.Database.GetCollection<BsonDocument>(_options.Prefix + ".migrationLock");
        }

        [Fact]
        public void Ctor_DatabaseIsNull_ThrowsAnException()
        {
            var exception = Assert.Throws<ArgumentNullException>(() => new MigrationLock(null, new MongoStorageOptions()));

            Assert.Equal("database", exception.ParamName);
        }

        [Fact]
        public void Ctor_StorageOptionsIsNull_ThrowsAnException_()
        {
            var exception = Assert.Throws<ArgumentNullException>(() => new MigrationLock(_database.Database, null));

            Assert.Equal("storageOptions", exception.ParamName);
        }

        [Fact]
        public void AcquireLock_NoLock_LockAcquired()
        {
            using var migrationLock = new MigrationLock(_database.Database, _options);
            migrationLock.AcquireLock();
            var locksCount = _locks.CountDocuments(new BsonDocument());
            Assert.Equal(1, locksCount);
            
            // Verify heartbeat is working by checking that ExpireAt gets updated
            var lockBefore = _locks.Find(new BsonDocument()).Single();
            var expireAtBefore = lockBefore[nameof(MigrationLockDto.ExpireAt)].ToUniversalTime();
            
            Thread.Sleep(TimeSpan.FromMilliseconds(500));
            
            var lockAfter = _locks.Find(new BsonDocument()).Single();
            var expireAtAfter = lockAfter[nameof(MigrationLockDto.ExpireAt)].ToUniversalTime();
            
            // ExpireAt should be updated by heartbeat
            Assert.True(expireAtAfter >= expireAtBefore);
        }

        [Fact]
        public void AcquireLock_Released_NoLock()
        {
            var filter = new BsonDocument();
            using (var migrationLock = new MigrationLock(_database.Database, _options))
            {
                migrationLock.AcquireLock();
                var locksCount = _locks.CountDocuments(filter);
                Assert.Equal(1, locksCount);
                // Small delay to allow heartbeat to initialize
                Thread.Sleep(TimeSpan.FromMilliseconds(50));
            }

            // After dispose, lock should be removed and heartbeat should be stopped
            var locksCountAfter = _locks.CountDocuments(filter);
            Assert.Equal(0, locksCountAfter);
        }

        [Fact]
        public void AcquireLock_TimesOut_ThrowsAnException()
        {
            // Use a longer timeout for the first lock so heartbeat can update it
            var options = new MongoStorageOptions{MigrationLockTimeout = TimeSpan.FromSeconds(5)};

            using var migrationLock = new MigrationLock(_database.Database, options);
            migrationLock.AcquireLock();
            var locksCount = _locks.CountDocuments(new BsonDocument());
            Assert.Equal(1, locksCount);

            var t = new Thread(() =>
            {
                // Second lock has very short timeout and can't acquire the lock held by first lock
                Assert.Throws<TimeoutException>(() =>
                    {
                        var options2 = new MongoStorageOptions{MigrationLockTimeout = TimeSpan.FromMilliseconds(50)};
                        using var migrationLock2 = new MigrationLock(_database.Database, options2);
                        migrationLock2.AcquireLock();
                    }
                );
            });
            t.Start();
            Assert.True(t.Join(5000), "Thread is hanging unexpected");
        }
        
        [Fact]
        public void AcquireLock_DoesNotAcquire_DoesNotDeleteLockInDb()
        {
            // Use longer timeout so heartbeat can keep the lock alive
            var options = new MongoStorageOptions{MigrationLockTimeout = TimeSpan.FromSeconds(5)};

            var migrationLock = new MigrationLock(_database.Database, options);
            migrationLock.AcquireLock();
            var locksCount = _locks.CountDocuments(new BsonDocument());
            Assert.Equal(1, locksCount);

            var t = new Thread(() =>
            {
                // Second lock has very short timeout so it will fail to acquire
                Assert.Throws<TimeoutException>(() =>
                    {
                        var options2 = new MongoStorageOptions{MigrationLockTimeout = TimeSpan.FromMilliseconds(50)};
                        using var migrationLock2 = new MigrationLock(_database.Database, options2);
                        migrationLock2.AcquireLock();
                    }
                );
                // Verify lock still exists (not deleted by failed acquisition attempt)
                locksCount = _locks.CountDocuments(new BsonDocument());
                Assert.Equal(1, locksCount);
            });
            t.Start();
            Assert.True(t.Join(5000), "Thread is hanging unexpected");
            
            migrationLock.Dispose();
        }

        [Fact]
        public void AcquireLock_SignaledAtLockRelease_WaitsForLock()
        {
            var t = new Thread(() =>
            {
                // Use longer timeout so the heartbeat can keep the lock alive
                var options = new MongoStorageOptions{MigrationLockTimeout = TimeSpan.FromSeconds(5)};

                using var migrationLock = new MigrationLock(_database.Database, options);
                migrationLock.AcquireLock();
                Thread.Sleep(TimeSpan.FromSeconds(3));
            });
            t.Start();

            // Wait just a bit to make sure the above lock is acquired
            Thread.Sleep(TimeSpan.FromSeconds(1));
            
            // Record when we try to acquire the lock
            var startTime = DateTime.UtcNow;
            using var migrationLock2 = new MigrationLock(_database.Database, _options);
            migrationLock2.AcquireLock();
            Assert.InRange(DateTime.UtcNow - startTime, TimeSpan.FromMilliseconds(1), TimeSpan.FromSeconds(5));
        }
        
        [Fact]
        public void AcquireLock_LockExpired_LockAcquired()
        {
            // simulate situation when lock was not disposed correctly (app crash) and there is no heartbeats to prolong ExpireAt value
            var initialExpireAt = DateTime.UtcNow.AddSeconds(3);
            _locks
                .InsertOne(new BsonDocument
                {
                    ["_id"] = MigrationLock.LockId,
                    [nameof(MigrationLockDto.ExpireAt)] = initialExpireAt
                });

            using var migrationLock = new MigrationLock(_database.Database, new MongoStorageOptions{MigrationLockTimeout = TimeSpan.FromSeconds(5)});
            migrationLock.AcquireLock();
            var lockEntry = new MigrationLockDto(_locks.Find(new BsonDocument()).Single());
            Assert.True(lockEntry.ExpireAt > initialExpireAt);
        }
    }
#pragma warning restore 1591
}