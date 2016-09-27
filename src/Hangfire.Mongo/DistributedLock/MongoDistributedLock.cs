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

        private static readonly ILog Logger = LogProvider.For<MongoDistributedLock>();

        private static readonly ThreadLocal<Dictionary<string, int>> AcquiredLocks
                    = new ThreadLocal<Dictionary<string, int>>(() => new Dictionary<string, int>());


        private readonly string _resource;

        private readonly HangfireDbContext _database;

        private readonly MongoStorageOptions _options;


        private Timer _heartbeatTimer;

        private bool _completed;

        private readonly object _lockObject = new object();

        private string EventWaitHandleName => $@"{GetType().FullName}.{_resource}";


        /// <summary>
        /// Creates MongoDB distributed lock
        /// </summary>
        /// <param name="resource">Lock resource</param>
        /// <param name="timeout">Lock timeout</param>
        /// <param name="database">Lock database</param>
        /// <param name="options">Database options</param>
        /// <exception cref="DistributedLockTimeoutException">Thrown if lock is not acuired within the timeout</exception>
        /// <exception cref="MongoDistributedLockException">Thrown if other mongo specific issue prevented the lock to be acquired</exception>
        public MongoDistributedLock(string resource, TimeSpan timeout, HangfireDbContext database, MongoStorageOptions options)
        {
            if (string.IsNullOrEmpty(resource))
            {
                throw new ArgumentNullException(nameof(resource));
            }
            if (timeout.TotalSeconds > int.MaxValue)
            {
                throw new ArgumentException($"The timeout specified is too large. Please supply a timeout equal to or less than {int.MaxValue} seconds", nameof(timeout));
            }
            if (database == null)
            {
                throw new ArgumentNullException(nameof(database));
            }
            if (options == null)
            {
                throw new ArgumentNullException(nameof(options));
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
                // If result is null, then it means we acquired the lock
                var isLockAcquired = false;
                var now = DateTime.Now;
                var lockTimeoutTime = now.Add(timeout);

                while (!isLockAcquired && (lockTimeoutTime >= now))
                {
                    // Acquire the lock if it does not exist - Notice: ReturnDocument.Before
                    var filter = Builders<DistributedLockDto>.Filter.Eq(_ => _.Resource, _resource);
                    var update = Builders<DistributedLockDto>.Update.SetOnInsert(_ => _.ExpireAt, _database.GetServerTimeUtc().Add(_options.DistributedLockLifetime));
                    var options = new FindOneAndUpdateOptions<DistributedLockDto>
                    {
                        IsUpsert = true,
                        ReturnDocument = ReturnDocument.Before
                    };
                    var result = _database.DistributedLock.FindOneAndUpdate(filter, update, options);

                    // If result is null, then it means we acquired the lock
                    if (result == null)
                    {
                        isLockAcquired = true;
                    }
                    else
                    {
                        // Wait on the event. This allows us to be "woken" up sooner rather than later.
                        // We wait in chunks as we need to "wake-up" from time to time and poll mongo,
                        // in case that the lock was acquired on another machine or instance.
                        var eventWaitHandle = new EventWaitHandle(false, EventResetMode.AutoReset, EventWaitHandleName);
                        eventWaitHandle.WaitOne((int)timeout.TotalMilliseconds / 10);
                        now = DateTime.Now;
                    }
                }

                if (!isLockAcquired)
                {
                    throw new DistributedLockTimeoutException($"Could not place a lock on the resource \'{_resource}\': The lock request timed out.");
                }
            }
            catch (DistributedLockTimeoutException)
            {
                throw;
            }
            catch (Exception ex)
            {
                throw new MongoDistributedLockException($"Could not place a lock on the resource \'{_resource}\': Check inner exception for details.", ex);
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
                EventWaitHandle eventWaitHandler;
                if (EventWaitHandle.TryOpenExisting(EventWaitHandleName, out eventWaitHandler))
                {
                    eventWaitHandler.Set();
                }
            }
            catch (Exception ex)
            {
                throw new MongoDistributedLockException($"Could not release a lock on the resource \'{_resource}\': Check inner exception for details.", ex);
            }
        }


        private void Cleanup()
        {
            try
            {
                // Delete expired locks
                _database.DistributedLock.DeleteOne(
                    Builders<DistributedLockDto>.Filter.Eq(_ => _.Resource, _resource) &
                    Builders<DistributedLockDto>.Filter.Lt(_ => _.ExpireAt, _database.GetServerTimeUtc()));
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
                        var filter = Builders<DistributedLockDto>.Filter.Eq(_ => _.Resource, _resource);
                        var update = Builders<DistributedLockDto>.Update.Set(_ => _.ExpireAt, _database.GetServerTimeUtc().Add(_options.DistributedLockLifetime));
                        _database.DistributedLock.FindOneAndUpdate(filter, update);
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
