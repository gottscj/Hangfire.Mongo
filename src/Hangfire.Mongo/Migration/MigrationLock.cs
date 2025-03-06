using System;
using System.Threading;
using Hangfire.Logging;
using Hangfire.Mongo.Dto;
using MongoDB.Bson;
using MongoDB.Driver;

namespace Hangfire.Mongo.Migration
{
    /// <summary>
    /// Migration lock handler
    /// </summary>
    public sealed class MigrationLock : IDisposable
    {
        /// <summary>
        /// ID used for migration locks
        /// </summary>
        public static readonly BsonObjectId LockId = new BsonObjectId(ObjectId.Parse("5c351d07197a9bcdba4832fc"));
        
        private static readonly ILog Logger = LogProvider.For<MigrationLock>();
        private readonly TimeSpan _timeout;
        private readonly IMongoCollection<BsonDocument> _migrationLock;
        private bool _disposed;
        
        /// <summary>
        /// ctor
        /// </summary>
        /// <param name="database"></param>
        /// <param name="storageOptions"></param>
        public MigrationLock(IMongoDatabase database, MongoStorageOptions storageOptions)
        {
            if (database is null)
            {
                throw new ArgumentNullException(nameof(database));
            }
            
            if (storageOptions is null)
            {
                throw new ArgumentNullException(nameof(storageOptions));
            }

            _timeout = storageOptions.MigrationLockTimeout; 
            _migrationLock = database.GetCollection<BsonDocument>(storageOptions.Prefix + ".migrationLock");
        }

        /// <summary>
        /// Acquires lock or throws TimeoutException
        /// </summary>
        public void AcquireLock()
        {
            if (_disposed)
            {
                throw new ObjectDisposedException("Lock is disposed");
            }
            
            try
            {
                // If result is null, then it means we acquired the lock
                var isLockAcquired = false;
                var now = DateTime.UtcNow;
                // wait maximum double of configured seconds for migration to complete
                var lockTimeoutTime = now.Add(_timeout);
                var filter = new BsonDocument("_id", LockId);
                // Acquire the lock if it does not exist - Notice: ReturnDocument.Before
                var options = new FindOneAndUpdateOptions<BsonDocument>
                {
                    IsUpsert = true,
                    ReturnDocument = ReturnDocument.Before
                };
                var update = new BsonDocument
                {
                    ["$setOnInsert"] = new BsonDocument
                    {
                        [nameof(MigrationLockDto.ExpireAt)] = lockTimeoutTime
                    }
                };
                // busy wait
                while (!isLockAcquired && lockTimeoutTime >= now)
                {
                    Cleanup();
                    try
                    {
                        var result = _migrationLock.FindOneAndUpdate(filter, update, options);

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
                            now = Wait();
                        }
                    }
                    catch (MongoCommandException)
                    {
                        // this can occur if two processes attempt to acquire a lock on the same resource simultaneously.
                        // unfortunately there doesn't appear to be a more specific exception type to catch.
                        now = Wait();
                    }
                }

                if (!isLockAcquired)
                {
                    throw new TimeoutException($"Could not complete migration. Never acquired lock within allowed time: {_timeout}\r\n" +
                                                        "Either another server did not complete the migration or migration was abruptly interrupted\r\n" +
                                                        $"If migration has been interrupted you need to manually delete '{_migrationLock.CollectionNamespace.CollectionName}' and start again.");
                }
            }
            catch (TimeoutException)
            {
                throw;
            }
            catch (Exception ex)
            {
                throw new TimeoutException(
                    "Could not complete migration: Check inner exception for details.", ex);
            }
        }

        private void Cleanup()
        {
            try
            {
                // Delete expired locks
                _migrationLock.DeleteOne(new BsonDocument
                {
                    ["_id"] = LockId,
                    [nameof(DistributedLockDto.ExpireAt)] = new BsonDocument
                    {
                        ["$lt"] = DateTime.UtcNow
                    }
                });
            }
            catch (Exception ex)
            {
                Logger.Error($"Unable to clean up locks on the migration lock. Details:\r\n{ex}");
            }
        }
        
        private DateTime Wait()
        {
            Thread.Sleep(TimeSpan.FromMilliseconds(100));
            return DateTime.UtcNow;
        }

        /// <inheritdoc />
        public void Dispose()
        {
            if (_disposed)
            {
                return;
            }

            _disposed = true;
            
            _migrationLock.DeleteOne(new BsonDocument("_id", LockId));
            Cleanup();
        }
    }
}