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
        private static readonly ILog Logger = LogProvider.For<MigrationLock>();
        private readonly TimeSpan _timeout;
        private readonly IMongoCollection<BsonDocument> _migrationLock;

        private readonly BsonDocument _migrationIdFilter =
            new BsonDocument("_id", new BsonObjectId(ObjectId.Parse("5c351d07197a9bcdba4832fc")));
        
        /// <summary>
        /// ctor
        /// </summary>
        /// <param name="database"></param>
        /// <param name="migrateLockCollectionPrefix"></param>
        /// <param name="timeout"></param>
        public MigrationLock(IMongoDatabase database, string migrateLockCollectionPrefix, TimeSpan timeout)
        {
            _timeout = timeout;
            _migrationLock = database.GetCollection<BsonDocument>(migrateLockCollectionPrefix + ".migrationLock");
        }

        /// <summary>
        /// Deletes migration lock, if any
        /// </summary>
        public void DeleteMigrationLock()
        {
            _migrationLock.DeleteOne(_migrationIdFilter);
        }

        /// <summary>
        /// Aquires lock or throws TimeoutException
        /// </summary>
        public void AcquireLock()
        {
            try
            {
                // If result is null, then it means we acquired the lock
                var isLockAcquired = false;
                var now = DateTime.UtcNow;
                // wait maximum double of configured seconds for migration to complete
                var lockTimeoutTime = now.Add(_timeout);
                // busy wait
                while (!isLockAcquired && (lockTimeoutTime >= now))
                {
                    // Acquire the lock if it does not exist - Notice: ReturnDocument.Before
                    var update = new BsonDocument
                    {
                        ["$setOnInsert"] = new BsonDocument
                        {
                            [nameof(MigrationLockDto.ExpireAt)] = lockTimeoutTime
                        }
                    };
                    
                    var options = new FindOneAndUpdateOptions<BsonDocument>
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

        private static DateTime Wait()
        {
            var milliSecondsTimeout = new Random().Next(10, 50);
            Thread.Sleep(milliSecondsTimeout);
            return DateTime.UtcNow;
        }
        
        /// <summary>
        /// Deletes lock
        /// </summary>
        public void Dispose()
        {
            DeleteMigrationLock();
        }
    }
}