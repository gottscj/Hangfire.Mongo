using Hangfire.Logging;
using Hangfire.Mongo.Database;
using Hangfire.Mongo.Dto;
using Hangfire.Mongo.MongoUtils;
using Hangfire.Mongo.Helpers;
using System;
using System.Threading;
using MongoDB.Driver;

namespace Hangfire.Mongo.DistributedLock
{
    /// <summary>
    /// Represents distibuted lock implementation for MongoDB
    /// </summary>
    public class MongoDistributedLock : IDisposable
    {
        private static readonly ILog Logger = LogProvider.GetCurrentClassLogger();

        private readonly HangfireDbContext _database;

        private readonly MongoStorageOptions _options;

        private readonly string _resource;

        private Timer _heartbeatTimer = null;

        private bool _completed;

        /// <summary>
        /// Creates MongoDB distributed lock
        /// </summary>
        /// <param name="resource">Lock resource</param>
        /// <param name="timeout">Lock timeout</param>
        /// <param name="database">Lock database</param>
        /// <param name="options">Database options</param>
        public MongoDistributedLock(string resource, TimeSpan timeout, HangfireDbContext database, MongoStorageOptions options)
        {
            if (String.IsNullOrEmpty(resource) == true)
                throw new ArgumentNullException("resource");

            if (database == null)
                throw new ArgumentNullException("database");

            if (options == null)
                throw new ArgumentNullException("options");

            _resource = resource;
            _database = database;
            _options = options;

            try
            {
                // Remove dead locks
                database.DistributedLock.DeleteManyAsync(
                    Builders<DistributedLockDto>.Filter.Eq(_ => _.Resource, resource) &
                    Builders<DistributedLockDto>.Filter.Lt(_ => _.Heartbeat, database.GetServerTimeUtc().Subtract(options.DistributedLockLifetime)));

                // Check lock
                DateTime lockTimeoutTime = DateTime.Now.Add(timeout);
                bool isLockedBySomeoneElse;
                bool isFirstAttempt = true;
                do
                {
                    isLockedBySomeoneElse = AsyncHelper.RunSync(() =>
                        database.DistributedLock
                            .Find(Builders<DistributedLockDto>.Filter.Eq(_ => _.Resource, resource) &
                                  Builders<DistributedLockDto>.Filter.Ne(_ => _.ClientId, _options.ClientId))
                            .FirstOrDefaultAsync()) != null;

                    if (isFirstAttempt == true)
                        isFirstAttempt = false;
                    else
                        Thread.Sleep((int)timeout.TotalMilliseconds / 10);
                }
                while ((isLockedBySomeoneElse == true) && (lockTimeoutTime >= DateTime.Now));

                // Set lock
                if (isLockedBySomeoneElse == false)
                {
                    AsyncHelper.RunSync(() => database.DistributedLock.FindOneAndUpdateAsync(
                        Builders<DistributedLockDto>.Filter.Eq(_ => _.Resource, resource),
                        Builders<DistributedLockDto>.Update.Combine(
                            Builders<DistributedLockDto>.Update.Set(_ => _.ClientId, _options.ClientId),
                            Builders<DistributedLockDto>.Update.Inc(_ => _.LockCount, 1),
                            Builders<DistributedLockDto>.Update.Set(_ => _.Heartbeat, database.GetServerTimeUtc())
                        ),
                        new FindOneAndUpdateOptions<DistributedLockDto> { IsUpsert = true }));

                    StartHeartBeat();
                }
                else
                {
                    throw new MongoDistributedLockException(String.Format("Could not place a lock on the resource '{0}': {1}.", _resource, "The lock request timed out"));
                }
            }
            catch (Exception ex)
            {
                if (ex is MongoDistributedLockException)
                    throw;
                else
                    throw new MongoDistributedLockException(String.Format("Could not place a lock on the resource '{0}': {1}.", _resource, "Check inner exception for details"), ex);
            }
        }

        /// <summary>
        /// Starts database heartbeat
        /// </summary>
        private void StartHeartBeat()
        {
            TimeSpan timerInterval = TimeSpan.FromMilliseconds(_options.DistributedLockLifetime.TotalMilliseconds / 5);

            _heartbeatTimer = new Timer(state =>
            {
                try
                {
                    AsyncHelper.RunSync(() => _database.DistributedLock.FindOneAndUpdateAsync(
                        Builders<DistributedLockDto>.Filter.Eq(_ => _.Resource, _resource) & Builders<DistributedLockDto>.Filter.Eq(_ => _.ClientId, _options.ClientId),
                        Builders<DistributedLockDto>.Update.Set(_ => _.Heartbeat, _database.GetServerTimeUtc())));
                }
                catch (Exception ex)
                {
                    Logger.ErrorFormat("Unable to update heartbeat on the resource '{0}'", ex, _resource);
                }
            }, null, timerInterval, timerInterval);
        }

        /// <summary>
        /// Disposes the object
        /// </summary>
        public void Dispose()
        {
            if (_completed)
                return;

            try
            {
                // Remove dead locks
                AsyncHelper.RunSync(() => _database.DistributedLock.DeleteManyAsync(
                    Builders<DistributedLockDto>.Filter.Eq(_ => _.Resource, _resource) &
                    Builders<DistributedLockDto>.Filter.Lt(_ => _.Heartbeat, _database.GetServerTimeUtc().Subtract(_options.DistributedLockLifetime))
                    ));

                // Remove resource lock
                AsyncHelper.RunSync(() => _database.DistributedLock.FindOneAndUpdateAsync(
                    Builders<DistributedLockDto>.Filter.Eq(_ => _.Resource, _resource) & Builders<DistributedLockDto>.Filter.Eq(_ => _.ClientId, _options.ClientId),
                    Builders<DistributedLockDto>.Update.Combine(
                        Builders<DistributedLockDto>.Update.Inc(_ => _.LockCount, -1),
                        Builders<DistributedLockDto>.Update.Set(_ => _.Heartbeat, _database.GetServerTimeUtc())
                    ),
                    new FindOneAndUpdateOptions<DistributedLockDto> { IsUpsert = true }
                    ));

                AsyncHelper.RunSync(() => _database.DistributedLock.FindOneAndDeleteAsync(
                    Builders<DistributedLockDto>.Filter.Lte(_ => _.LockCount, 0)
                    ));

                if (_heartbeatTimer != null)
                {
                    _heartbeatTimer.Dispose();
                    _heartbeatTimer = null;
                }

                _completed = true;
            }
            catch (Exception ex)
            {
                throw new MongoDistributedLockException(String.Format("Could not release a lock on the resource '{0}': {1}.", _resource, "Check inner exception for details"), ex);
            }
        }
    }
}