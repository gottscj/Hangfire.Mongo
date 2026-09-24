using System;
using System.Collections.Generic;
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

        // Reentrant acquisitions on a thread share the lock document written by the first one,
        // so its owner token is tracked per thread and resource, like the base reentrancy count.
        private static readonly ThreadLocal<Dictionary<string, string>> OwnerTokens
            = new ThreadLocal<Dictionary<string, string>>(() => new Dictionary<string, string>());

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
            OwnerTokens.Value[_resource] = _ownerToken;
            var interval = TimeSpan.FromTicks(_storageOptions.DistributedLockLifetime.Ticks / 5);
            _heartbeat = AsyncHeartbeat.Start(interval, ExtendLockAsync,
                ex => Logger.Error($"{_resource} - Unable to update heartbeat on the resource. Details:\r\n{ex}"));
        }

        /// <summary>
        /// Release the lock if it is still owned by the instance which acquired it on this thread
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

                var ownerTokens = OwnerTokens.Value;
                if (!ownerTokens.TryGetValue(_resource, out var ownerToken))
                {
                    ownerToken = _ownerToken;
                }

                ownerTokens.Remove(_resource);
                _dbContext.DistributedLock.DeleteOne(CreateOwnerFilter(ownerToken));
            }
            catch (Exception ex)
            {
                throw new MongoDistributedLockException($"{_resource} - Could not release lock.", ex);
            }
        }

        private async Task ExtendLockAsync()
        {
            var update = new BsonDocument
            {
                ["$set"] = new BsonDocument
                {
                    [nameof(DistributedLockDto.ExpireAt)] = DateTime.UtcNow.Add(_storageOptions.DistributedLockLifetime)
                }
            };
            var result = await _dbContext.DistributedLock
                .UpdateOneAsync(CreateOwnerFilter(_ownerToken), update)
                .ConfigureAwait(false);

            // No match means the lock expired and was removed or taken by another owner. It cannot
            // come back with our token, so there is nothing left to keep alive. After Stop, no match
            // is expected because Dispose releases the lock.
            if (result.MatchedCount == 0 && _heartbeat is { IsStopped: false } heartbeat)
            {
                heartbeat.Stop();
                Logger.Warn($"{_resource} - Lock was lost: it expired or is now held by another owner.");
            }
        }

        private BsonDocument CreateOwnerFilter(string ownerToken)
        {
            return new BsonDocument
            {
                [nameof(DistributedLockDto.Resource)] = _resource,
                [nameof(DistributedLockDto.OwnerToken)] = ownerToken
            };
        }
    }
}
