using System;
using System.Collections.Generic;
using System.Threading;
using Hangfire.Logging;
using Hangfire.Mongo.Database;
using Hangfire.Mongo.Dto;
using Hangfire.Mongo.MongoUtils;
using Hangfire.Storage;
using MongoDB.Driver;

namespace Hangfire.Mongo.DistributedLock
{
    /// <summary>
    /// Represents distibuted lock implementation for MongoDB
    /// </summary>
    public sealed class MongoDistributedLock : IDisposable
    {

        private static readonly ILog Logger = LogProvider.GetCurrentClassLogger();

        private static readonly ThreadLocal<Dictionary<string, int>> AcquiredLocks
                    = new ThreadLocal<Dictionary<string, int>>(() => new Dictionary<string, int>());


        private readonly string _resource;

        private readonly HangfireDbContext _database;

        private readonly MongoStorageOptions _options;


        private Timer _heartbeatTimer;

        private bool _completed;

        private readonly object _lockObject = new object();


        /// <summary>
        /// Creates MongoDB distributed lock
        /// </summary>
        /// <param name="resource">Lock resource</param>
        /// <param name="timeout">Lock timeout</param>
        /// <param name="database">Lock database</param>
        /// <param name="options">Database options</param>
        /// <exception cref="DistributedLockTimeoutException">Thrown if lock is not acuired within the timeout</exception>
        /// <exception cref="MongoDistributedLockException">Thrown if other mongo specific issue prevented the lock to be aquired</exception>
        public MongoDistributedLock(string resource, TimeSpan timeout, HangfireDbContext database, MongoStorageOptions options)
        {
            if (string.IsNullOrEmpty(resource))
            {
                throw new ArgumentNullException("resource");
            }
            if (timeout.TotalSeconds > int.MaxValue)
            {
                throw new ArgumentException(string.Format("The timeout specified is too large. Please supply a timeout equal to or less than {0} seconds", int.MaxValue), "timeout");
            }
            if (database == null)
            {
                throw new ArgumentNullException("database");
            }
            if (options == null)
            {
                throw new ArgumentNullException("options");
            }

            _resource = resource;
            _database = database;
            _options = options;

            if (!AcquiredLocks.Value.ContainsKey(_resource) || AcquiredLocks.Value[_resource] == 0)
            {
                Cleanup();
                Acquire(timeout);
                AcquiredLocks.Value[_resource] = 1;
                StartHeartBeat();
            }
            else
            {
                AcquiredLocks.Value[_resource]++;
            }
        }


        /// <summary>
        /// Disposes the object
        /// </summary>
        /// <exception cref="MongoDistributedLockException"></exception>
        public void Dispose()
        {
            if (_completed)
            {
                return;
            }
            _completed = true;

            if (!AcquiredLocks.Value.ContainsKey(_resource))
            {
                return;
            }

            AcquiredLocks.Value[_resource]--;

            if (AcquiredLocks.Value[_resource] > 0)
            {
                return;
            }

            // Timer callback may be invoked after the Dispose method call,
            // so we are using lock to avoid unsynchronized calls.
            lock (_lockObject)
            {
                AcquiredLocks.Value.Remove(_resource);

                if (_heartbeatTimer != null)
                {
                    _heartbeatTimer.Dispose();
                    _heartbeatTimer = null;
                }

                Release();

                Cleanup();
            }
        }


        private void Acquire(TimeSpan timeout)
        {
            try
            {
                // If result is null, then it means we aquired the lock
                bool isLockAquired = false;
                DateTime now = DateTime.Now;
                DateTime lockTimeoutTime = now.Add(timeout);

                while (!isLockAquired && (lockTimeoutTime >= now))
                {
                    // Aquire the lock if it does not exist - Notice: ReturnDocument.Before
                    DistributedLockDto result = _database.DistributedLock.FindOneAndUpdate(
                        Builders<DistributedLockDto>.Filter.Eq(_ => _.Resource, _resource),
                        Builders<DistributedLockDto>.Update.Combine(
                            Builders<DistributedLockDto>.Update.SetOnInsert(_ => _.Heartbeat, _database.GetServerTimeUtc())),
                        new FindOneAndUpdateOptions<DistributedLockDto> { IsUpsert = true, ReturnDocument = ReturnDocument.Before });

                    // If result is null, then it means we aquired the lock
                    if (result == null)
                    {
                        isLockAquired = true;
                    }
                    else
                    {
                        Thread.Sleep((int)timeout.TotalMilliseconds / 10);
                        now = DateTime.Now;
                    }
                }

                if (!isLockAquired)
                {
                    throw new DistributedLockTimeoutException(
                        string.Format("Could not place a lock on the resource '{0}': {1}.", _resource,
                            "The lock request timed out"));
                }
            }
            catch (DistributedLockTimeoutException)
            {
                throw;
            }
            catch (Exception ex)
            {
                throw new MongoDistributedLockException(
                    string.Format("Could not place a lock on the resource '{0}': {1}.", _resource,
                        "Check inner exception for details"), ex);
            }
        }


        /// <summary>
        /// Release the lock
        /// </summary>
        /// <exception cref="MongoDistributedLockException"></exception>
        private void Release()
        {
            try
            {
                // Remove resource lock
                _database.DistributedLock.DeleteOne(
                    Builders<DistributedLockDto>.Filter.Eq(_ => _.Resource, _resource));
            }
            catch (Exception ex)
            {
                throw new MongoDistributedLockException(
                    string.Format("Could not release a lock on the resource '{0}': {1}.", _resource,
                        "Check inner exception for details"), ex);
            }
        }


        private void Cleanup()
        {
            try
            {
                // Delete expired locks
                _database.DistributedLock.DeleteOne(
                    Builders<DistributedLockDto>.Filter.Eq(_ => _.Resource, _resource) &
                    Builders<DistributedLockDto>.Filter.Lt(_ => _.Heartbeat, _database.GetServerTimeUtc().Subtract(_options.DistributedLockLifetime)));
            }
            catch (Exception ex)
            {
                Logger.ErrorFormat("Unable to clean up locks on the resource '{0}'. {1}", _resource, ex);
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
                // Timer callback may be invoked after the Dispose method call,
                // so we are using lock to avoid unsynchronized calls.
                lock (_lockObject)
                {
                    try
                    {
                        _database.DistributedLock
                            .FindOneAndUpdate(Builders<DistributedLockDto>.Filter.Eq(_ => _.Resource, _resource),
                                Builders<DistributedLockDto>.Update.Set(_ => _.Heartbeat, _database.GetServerTimeUtc()));
                    }
                    catch (Exception ex)
                    {
                        Logger.ErrorFormat("Unable to update heartbeat on the resource '{0}'. {1}", _resource, ex);
                    }
                }
            }, null, timerInterval, timerInterval);
        }

    }
}
