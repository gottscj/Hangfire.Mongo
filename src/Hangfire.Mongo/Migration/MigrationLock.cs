using System;
using System.Threading;
using Hangfire.Logging;
using Hangfire.Mongo.Dto;
using MongoDB.Bson;
using MongoDB.Driver;

namespace Hangfire.Mongo.Migration
{
    internal sealed class MigrationLock : IDisposable
    {
        private static readonly ILog Logger = LogProvider.For<MigrationLock>();
        private readonly TimeSpan _timeout;
        private readonly IMongoCollection<MigrationLockDto> _migrationLock;

        private readonly BsonDocument _migrationIdFilter =
            new BsonDocument("_id", new BsonObjectId("5c351d07197a9bcdba4832fc"));
        
        public MigrationLock(IMongoDatabase database, string migrateLockCollectionName, TimeSpan timeout)
        {
            _timeout = timeout;
            _migrationLock = database.GetCollection<MigrationLockDto>(migrateLockCollectionName);
        }
        
        public void AcquireMigrationAccess()
        {
            try
            {
                // If result is null, then it means we acquired the lock
                var isLockAcquired = false;
                var now = DateTime.UtcNow;
                // wait maximum double of configured seconds for migration to complete
                var lockTimeoutTime = now.Add(_timeout);

                var deleteFilter = new BsonDocument("$and", new BsonArray
                {
                    _migrationIdFilter,
                    new BsonDocument(nameof(MigrationLockDto.ExpireAt), new BsonDocument("$lt", DateTime.UtcNow))
                });

                _migrationLock.DeleteOne(deleteFilter);
                
                // busy wait
                while (!isLockAcquired && (lockTimeoutTime >= now))
                {
                    // Acquire the lock if it does not exist - Notice: ReturnDocument.Before
                    var update = Builders<MigrationLockDto>
                        .Update
                        .SetOnInsert(_ => _.ExpireAt, lockTimeoutTime);
                    
                    var options = new FindOneAndUpdateOptions<MigrationLockDto>
                    {
                        IsUpsert = true,
                        ReturnDocument = ReturnDocument.Before
                    };

                    try
                    {
                        var result = _migrationLock.FindOneAndUpdate(_migrationIdFilter, update, options);

                        // If result is null, it means we acquired the lock
                        if (result == null)
                        {
                            if (Logger.IsDebugEnabled())
                            {
                                Logger.Debug("Acquired lock for migration");
                            }

                            isLockAcquired = true;
                        }
                        else
                        {
                            Thread.Sleep(20);
                            now = DateTime.UtcNow;
                        }
                    }
                    catch (MongoCommandException)
                    {
                        // this can occur if two processes attempt to acquire a lock on the same resource simultaneously.
                        // unfortunately there doesn't appear to be a more specific exception type to catch.
                        Thread.Sleep(20);
                        now = DateTime.UtcNow;
                    }
                }

                if (!isLockAcquired)
                {
                    throw new InvalidOperationException($"Could not complete migration. Never acquired lock within allowed time: {_timeout}");
                }
            }
            catch (InvalidOperationException)
            {
                throw;
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException(
                    "Could not complete migration: Check inner exception for details.", ex);
            }
        }
        public void Dispose()
        {
            _migrationLock.DeleteOne(new BsonDocument("_id", _migrationIdFilter));
        }
    }
}