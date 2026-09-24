using System;
using System.Threading;
using System.Threading.Tasks;
using Hangfire.Logging;
using Hangfire.Mongo.Database;
using Hangfire.Mongo.Dto;
using MongoDB.Bson;

namespace Hangfire.Mongo.DistributedLock
{
    /// <summary>
    /// Distributed lock which keeps the lock alive with an asynchronous heartbeat instead of a
    /// timer callback running a synchronous database call on a thread-pool thread.
    /// Each lock records an owner token, and the heartbeat and release only touch the lock
    /// document while it still carries that token, so a lock that expired and was acquired by
    /// another owner is neither extended nor deleted.
    /// </summary>
    public class AsyncMongoDistributedLock : MongoDistributedLock
    {
        private static readonly ILog Logger = LogProvider.For<AsyncMongoDistributedLock>();

        private readonly string _resource;
        private readonly HangfireDbContext _dbContext;
        private readonly MongoStorageOptions _storageOptions;
        private readonly string _ownerToken = Guid.NewGuid().ToString("N");

        private AsyncHeartbeat _heartbeat;

        /// <summary>
        /// Creates MongoDB distributed lock
        /// </summary>
        /// <param name="resource">Lock resource</param>
        /// <param name="timeout">Lock timeout</param>
        /// <param name="dbContext"></param>
        /// <param name="storageOptions">Database options</param>
        public AsyncMongoDistributedLock(string resource,
            TimeSpan timeout,
            HangfireDbContext dbContext,
            MongoStorageOptions storageOptions)
            : base(resource, timeout, dbContext, storageOptions)
        {
            _resource = resource;
            _dbContext = dbContext;
            _storageOptions = storageOptions;
        }

        /// <summary>
        /// Disposes the object
        /// </summary>
        public override void Dispose()
        {
            _heartbeat?.Stop();
            base.Dispose();
        }

        /// <inheritdoc />
        protected override BsonDocument CreateLockFields()
        {
            var fields = base.CreateLockFields();
            fields[nameof(DistributedLockDto.OwnerToken)] = _ownerToken;
            return fields;
        }

        /// <inheritdoc />
        protected override void StartHeartBeat()
        {
            var interval = TimeSpan.FromTicks(_storageOptions.DistributedLockLifetime.Ticks / 5);
            _heartbeat = AsyncHeartbeat.Start(interval, ExtendLockAsync,
                ex => Logger.Error($"{_resource} - Unable to update heartbeat on the resource. Details:\r\n{ex}"));
        }

        /// <summary>
        /// Release the lock if it is still owned by this instance
        /// </summary>
        /// <exception cref="MongoDistributedLockException"></exception>
        protected override void Release()
        {
            try
            {
                if (Logger.IsTraceEnabled())
                {
                    Logger.Trace($"{_resource} - Release");
                }

                _dbContext.DistributedLock.DeleteOne(CreateOwnerFilter());
            }
            catch (Exception ex)
            {
                throw new MongoDistributedLockException($"{_resource} - Could not release lock.", ex);
            }
        }

        private async Task ExtendLockAsync(CancellationToken cancellationToken)
        {
            var update = new BsonDocument
            {
                ["$set"] = new BsonDocument
                {
                    [nameof(DistributedLockDto.ExpireAt)] = DateTime.UtcNow.Add(_storageOptions.DistributedLockLifetime)
                }
            };
            var result = await _dbContext.DistributedLock
                .UpdateOneAsync(CreateOwnerFilter(), update, cancellationToken: cancellationToken)
                .ConfigureAwait(false);

            // No match means the lock expired and was removed or taken by another owner. It cannot
            // come back with our token, so there is nothing left to keep alive. After Stop, no match
            // is expected because Dispose releases the lock.
            if (result.MatchedCount == 0 && !cancellationToken.IsCancellationRequested)
            {
                _heartbeat?.Stop();
                Logger.Warn($"{_resource} - Lock was lost: it expired or is now held by another owner.");
            }
        }

        private BsonDocument CreateOwnerFilter()
        {
            return new BsonDocument
            {
                [nameof(DistributedLockDto.Resource)] = _resource,
                [nameof(DistributedLockDto.OwnerToken)] = _ownerToken
            };
        }
    }
}
