using System;
using System.Collections.Generic;
using System.Threading;
using Hangfire.Logging;
using Hangfire.Mongo.Database;
using Hangfire.Mongo.Dto;
using Hangfire.Storage;
using MongoDB.Bson;
using MongoDB.Driver;

namespace Hangfire.Mongo.DistributedLock
{
    /// <summary>
    /// Represents distributed lock implementation for MongoDB
    /// </summary>
    internal sealed class MongoDistributedLock : IDisposable
    {
        private static readonly ILog Logger = LogProvider.For<MongoDistributedLock>();

        private static readonly ThreadLocal<Dictionary<string, int>> AcquiredLocks
                    = new ThreadLocal<Dictionary<string, int>>(() => new Dictionary<string, int>());


        private readonly string _resource;

        private readonly IMongoCollection<DistributedLockDto> _locks;
        private readonly IMongoCollection<NotificationDto> _notifications;

        private readonly MongoStorageOptions _storageOptions;
        private readonly IDistributedLockMutex _mutex;


        private Timer _heartbeatTimer;

        private bool _completed;

        private readonly object _lockObject = new object();

        /// <summary>
        /// Creates MongoDB distributed lock
        /// </summary>
        /// <param name="resource">Lock resource</param>
        /// <param name="timeout">Lock timeout</param>
        /// <param name="dbContext">Lock Database</param>
        /// <param name="storageOptions">Database options</param>
        /// <param name="mutex"></param>
        /// <exception cref="DistributedLockTimeoutException">Thrown if lock is not acquired within the timeout</exception>
        /// <exception cref="MongoDistributedLockException">Thrown if other mongo specific issue prevented the lock to be acquired</exception>
        public MongoDistributedLock(string resource,
            TimeSpan timeout, 
            HangfireDbContext dbContext,
            MongoStorageOptions storageOptions,
            IDistributedLockMutex mutex) 
            : this(resource, timeout, dbContext?.DistributedLock, dbContext?.Notifications, storageOptions, mutex)
        {

        }

        /// <summary>
        /// Creates MongoDB distributed lock
        /// </summary>
        /// <param name="resource">Lock resource</param>
        /// <param name="timeout">Lock timeout</param>
        /// <param name="locks">Lock collection</param>
        /// <param name="notifications"></param>
        /// <param name="storageOptions">Database options</param>
        /// <param name="mutex"></param>
        /// <exception cref="DistributedLockTimeoutException">Thrown if lock is not acquired within the timeout</exception>
        /// <exception cref="MongoDistributedLockException">Thrown if other mongo specific issue prevented the lock to be acquired</exception>
        public MongoDistributedLock(string resource, 
            TimeSpan timeout, 
            IMongoCollection<DistributedLockDto> locks,
            IMongoCollection<NotificationDto> notifications,
            MongoStorageOptions storageOptions,
            IDistributedLockMutex mutex)
        {
            _resource = resource ?? throw new ArgumentNullException(nameof(resource));
            _locks = locks ?? throw new ArgumentNullException(nameof(locks));
            _notifications = notifications;
            _storageOptions = storageOptions ?? throw new ArgumentNullException(nameof(storageOptions));
            _mutex = mutex;

            if (string.IsNullOrEmpty(resource))
            {
                throw new ArgumentException($@"The {nameof(resource)} cannot be empty", nameof(resource));
            }
            if (timeout.TotalSeconds > int.MaxValue)
            {
                throw new ArgumentException($"The timeout specified is too large. Please supply a timeout equal to or less than {int.MaxValue} seconds", nameof(timeout));
            }

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
            // so we are using lock to avoid un synchronized calls.
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
                var now = DateTime.UtcNow;
                var lockTimeoutTime = now.Add(timeout);
                while (!isLockAcquired && (lockTimeoutTime >= now))
                {
                    // Acquire the lock if it does not exist - Notice: ReturnDocument.Before
                    var filter = Builders<DistributedLockDto>.Filter.Eq(_ => _.Resource, _resource);
                    var update = Builders<DistributedLockDto>.Update.SetOnInsert(_ => _.ExpireAt, DateTime.UtcNow.Add(_storageOptions.DistributedLockLifetime));
                    var options = new FindOneAndUpdateOptions<DistributedLockDto>
                    {
                        IsUpsert = true,
                        ReturnDocument = ReturnDocument.Before
                    };

                    try
                    {
                        var result = _locks.FindOneAndUpdate(filter, update, options);
                        
                        // If result is null, it means we acquired the lock
                        if (result == null)
                        {
                            if (Logger.IsDebugEnabled())
                            {
                                Logger.Debug($"{_resource} - Acquired");    
                            }
                            isLockAcquired = true;
                        }
                        else
                        {
                            now = _mutex.Wait(_resource, CalculateTimeout(timeout));
                        }
                    }
                    catch (MongoCommandException)
                    {
                        // this can occur if two processes attempt to acquire a lock on the same resource simultaneously.
                        // unfortunately there doesn't appear to be a more specific exception type to catch.
                        now = _mutex.Wait(_resource, CalculateTimeout(timeout));
                    }
                }

                if (!isLockAcquired)
                {
                    throw new DistributedLockTimeoutException($"{_resource} - Could not place a lock: The lock request timed out.");
                }
            }
            catch (DistributedLockTimeoutException)
            {
                throw;
            }
            catch (Exception ex)
            {
                throw new MongoDistributedLockException($"{_resource} - Could not place a lock", ex);
            }
        }

        private static TimeSpan CalculateTimeout(TimeSpan timeout)
        {
            return TimeSpan.FromMilliseconds((timeout.TotalMilliseconds / 1000) + 5);
        }

        /// <summary>
        /// Release the lock
        /// </summary>
        /// <exception cref="MongoDistributedLockException"></exception>
        private void Release()
        {
            try
            {
                if (Logger.IsDebugEnabled())
                {
                    Logger.Debug($"{_resource} - Release");    
                }
                // Remove resource lock
                _locks.DeleteOne(new BsonDocument(nameof(DistributedLockDto.Resource), _resource));
                _notifications.InsertOne(NotificationDto.LockReleased(_resource), new InsertOneOptions
                {
                    BypassDocumentValidation = false
                });
            }
            catch (Exception ex)
            {
                throw new MongoDistributedLockException($"{_resource} - Could not release lock.", ex);
            }
        }


        private void Cleanup()
        {
            try
            {
                // Delete expired locks
                _locks.DeleteOne(
                    Builders<DistributedLockDto>.Filter.Eq(_ => _.Resource, _resource) &
                    Builders<DistributedLockDto>.Filter.Lt(_ => _.ExpireAt, DateTime.UtcNow));
            }
            catch (Exception ex)
            {
                Logger.Error($"{_resource} - Unable to clean up locks on the resource. Details:\r\n{ex}");
            }
        }

        /// <summary>
        /// Starts database heartbeat
        /// </summary>
        private void StartHeartBeat()
        {
            TimeSpan timerInterval = TimeSpan.FromMilliseconds(_storageOptions.DistributedLockLifetime.TotalMilliseconds / 5);
            _heartbeatTimer = new Timer(state =>
            {
                // Timer callback may be invoked after the Dispose method call,
                // so we are using lock to avoid un synchronized calls.
                lock (_lockObject)
                {
                    try
                    {
                        var filter = Builders<DistributedLockDto>.Filter.Eq(_ => _.Resource, _resource);
                        var update = Builders<DistributedLockDto>.Update.Set(_ => _.ExpireAt, DateTime.UtcNow.Add(_storageOptions.DistributedLockLifetime));
                        _locks.FindOneAndUpdate(filter, update);
                    }
                    catch (Exception ex)
                    {
                        Logger.Error($"{_resource} - Unable to update heartbeat on the resource. Details:\r\n{ex}");
                    }
                }
            }, null, timerInterval, timerInterval);
        }
    }
}