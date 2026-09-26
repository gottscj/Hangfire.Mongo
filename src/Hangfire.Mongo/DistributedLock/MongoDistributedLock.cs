using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
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
    public class MongoDistributedLock : IDisposable
    {
        private static readonly ILog Logger = LogProvider.For<MongoDistributedLock>();

        private static readonly ThreadLocal<Dictionary<string, int>> AcquiredLocks
            = new ThreadLocal<Dictionary<string, int>>(() => new Dictionary<string, int>());


        private readonly string _resource;
        private readonly TimeSpan _timeout;
        private readonly HangfireDbContext _dbContext;
        private readonly MongoStorageOptions _storageOptions;
        private readonly string _ownerToken = Guid.NewGuid().ToString("N");

        private CancellationTokenSource _heartbeatCancellation;
        private Task _heartbeatTask;

        private bool _completed;

        private readonly object _lockObject = new object();

        /// <summary>
        /// Creates MongoDB distributed lock
        /// </summary>
        /// <param name="resource">Lock resource</param>
        /// <param name="timeout">Lock timeout</param>
        /// <param name="dbContext"></param>
        /// <param name="storageOptions">Database options</param>
        /// <exception cref="DistributedLockTimeoutException">Thrown if lock is not acquired within the timeout</exception>
        /// <exception cref="MongoDistributedLockException">Thrown if other mongo specific issue prevented the lock to be acquired</exception>
        public MongoDistributedLock(string resource,
            TimeSpan timeout,
            HangfireDbContext dbContext,
            MongoStorageOptions storageOptions)
        {
            _resource = resource ?? throw new ArgumentNullException(nameof(resource));
            _timeout = timeout;
            _dbContext = dbContext ?? throw new ArgumentNullException(nameof(dbContext));
            _storageOptions = storageOptions ?? throw new ArgumentNullException(nameof(storageOptions));

            if (string.IsNullOrEmpty(resource))
            {
                throw new ArgumentException($@"The {nameof(resource)} cannot be empty", nameof(resource));
            }

            if (timeout.TotalSeconds > int.MaxValue)
            {
                throw new ArgumentException(
                    $"The timeout specified is too large. Please supply a timeout equal to or less than {int.MaxValue} seconds",
                    nameof(timeout));
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public virtual IDisposable AcquireLock()
        {
            if (!AcquiredLocks.Value.ContainsKey(_resource) || AcquiredLocks.Value[_resource] == 0)
            {
                Cleanup();
                Acquire(_timeout);
                AcquiredLocks.Value[_resource] = 1;
                StartHeartBeat();
            }
            else
            {
                AcquiredLocks.Value[_resource]++;
            }

            return this;
        }

        /// <summary>
        /// Disposes the object
        /// </summary>
        /// <exception cref="MongoDistributedLockException"></exception>
        public virtual void Dispose()
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

            StopHeartbeat();

            lock (_lockObject)
            {
                AcquiredLocks.Value.Remove(_resource);

                Release();

                Cleanup();
            }
        }


        /// <summary>
        /// Acquire lock
        /// </summary>
        /// <param name="timeout"></param>
        /// <exception cref="DistributedLockTimeoutException"></exception>
        /// <exception cref="MongoDistributedLockException"></exception>
        protected virtual void Acquire(TimeSpan timeout)
        {
            try
            {
                // If result is null, then it means we acquired the lock
                var isLockAcquired = false;
                var now = DateTime.UtcNow;
                var lockTimeoutTime = now.Add(timeout);
                var filter = new BsonDocument
                {
                    [nameof(DistributedLockDto.Resource)] = _resource
                };

                var options = new FindOneAndUpdateOptions<BsonDocument>
                {
                    IsUpsert = true,
                    ReturnDocument = ReturnDocument.Before
                };
                while (!isLockAcquired && (lockTimeoutTime >= now))
                {
                    // Acquire the lock if it does not exist - Notice: ReturnDocument.Before

                    var update = new BsonDocument
                    {
                        ["$setOnInsert"] = new BsonDocument
                        {
                            [nameof(DistributedLockDto.OwnerToken)] = _ownerToken,
                            [nameof(DistributedLockDto.ExpireAt)] =
                                DateTime.UtcNow.Add(_storageOptions.DistributedLockLifetime)
                        }
                    };
                    try
                    {
                        var result = _dbContext.DistributedLock.FindOneAndUpdate(filter, update, options);

                        // If result is null, it means we acquired the lock
                        if (result == null)
                        {
                            if (Logger.IsTraceEnabled())
                            {
                                Logger.Trace($"{_resource} - Acquired");
                            }

                            isLockAcquired = true;
                        }
                        else
                        {
                            now = Wait(_resource, CalculateTimeout(timeout));
                        }
                    }
                    catch (MongoCommandException)
                    {
                        // this can occur if two processes attempt to acquire a lock on the same resource simultaneously.
                        // unfortunately there doesn't appear to be a more specific exception type to catch.
                        now = Wait(_resource, CalculateTimeout(timeout));
                    }

                    Cleanup();
                }

                if (!isLockAcquired)
                {
                    throw new DistributedLockTimeoutException(
                        $"{_resource} - Could not place a lock: The lock request timed out.");
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

        /// <summary>
        /// Calculates timeout, same as Hangfire.SqlServer
        /// </summary>
        /// <param name="timeout"></param>
        /// <returns></returns>
        protected virtual TimeSpan CalculateTimeout(TimeSpan timeout)
        {
            return TimeSpan.FromMilliseconds((timeout.TotalMilliseconds / 1000) + 5);
        }

        /// <summary>
        /// Release the lock
        /// </summary>
        /// <exception cref="MongoDistributedLockException"></exception>
        protected virtual void Release()
        {
            try
            {
                if (Logger.IsTraceEnabled())
                {
                    Logger.Trace($"{_resource} - Release");
                }

                // Remove resource lock
                _dbContext.DistributedLock.DeleteOne(new BsonDocument
                {
                    [nameof(DistributedLockDto.Resource)] = _resource,
                    [nameof(DistributedLockDto.OwnerToken)] = _ownerToken
                });
            }
            catch (Exception ex)
            {
                throw new MongoDistributedLockException($"{_resource} - Could not release lock.", ex);
            }
        }


        /// <summary>
        /// Delete expired locks
        /// </summary>
        protected virtual void Cleanup()
        {
            try
            {
                // Delete expired locks
                _dbContext.DistributedLock.DeleteOne(new BsonDocument
                {
                    [nameof(DistributedLockDto.Resource)] = _resource,
                    [nameof(DistributedLockDto.ExpireAt)] = new BsonDocument
                    {
                        ["$lt"] = DateTime.UtcNow
                    }
                });
            }
            catch (Exception ex)
            {
                Logger.Error($"{_resource} - Unable to clean up locks on the resource. Details:\r\n{ex}");
            }
        }

        /// <summary>
        /// Starts database heartbeat
        /// </summary>
        protected virtual void StartHeartBeat()
        {
            var timerInterval =
                TimeSpan.FromMilliseconds(_storageOptions.DistributedLockLifetime.TotalMilliseconds / 5);
            _heartbeatCancellation = new CancellationTokenSource();
            _heartbeatTask = RunHeartbeatAsync(timerInterval, _heartbeatCancellation.Token);
        }

        private async Task RunHeartbeatAsync(TimeSpan timerInterval, CancellationToken cancellationToken)
        {
            while (true)
            {
                try
                {
                    await Task.Delay(timerInterval, cancellationToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    return;
                }

                if (Volatile.Read(ref _completed))
                {
                    return;
                }

                try
                {
                    var filter = new BsonDocument
                    {
                        [nameof(DistributedLockDto.Resource)] = _resource,
                        [nameof(DistributedLockDto.OwnerToken)] = _ownerToken
                    };
                    var update = new BsonDocument
                    {
                        ["$set"] = new BsonDocument
                        {
                            [nameof(DistributedLockDto.ExpireAt)] = DateTime
                                .UtcNow.Add(_storageOptions.DistributedLockLifetime)
                        }
                    };

                    Stopwatch sw = null;
                    if (Logger.IsTraceEnabled())
                    {
                        sw = Stopwatch.StartNew();
                    }

                    await _dbContext.DistributedLock
                        .UpdateOneAsync(filter, update, cancellationToken: cancellationToken)
                        .ConfigureAwait(false);

                    if (Logger.IsTraceEnabled() && sw != null)
                    {
                        var serializedModel = new Dictionary<string, BsonDocument>
                        {
                            ["Filter"] = filter,
                            ["Update"] = update
                        };
                        sw.Stop();
                        var builder = new StringBuilder();
                        builder.AppendLine($"Lock heartbeat");
                        builder.AppendLine($"{serializedModel.ToJson()}");
                        builder.AppendLine($"Executed in {sw.ElapsedMilliseconds} ms");
                        Logger.Trace($"{builder}");
                    }
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    return;
                }
                catch (Exception ex)
                {
                    Logger.Error($"{_resource} - Unable to update heartbeat on the resource. Details:\r\n{ex}");
                }
            }
        }

        private void StopHeartbeat()
        {
            var cancellation = _heartbeatCancellation;
            var heartbeat = _heartbeatTask;
            _heartbeatCancellation = null;
            _heartbeatTask = null;
            cancellation?.Cancel();
            heartbeat?.GetAwaiter().GetResult();
            cancellation?.Dispose();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="resource"></param>
        /// <param name="timeout"></param>
        /// <returns></returns>
        protected virtual DateTime Wait(string resource, TimeSpan timeout)
        {
            if (Logger.IsTraceEnabled())
            {
                Logger.Trace($"{resource} - Waiting {timeout.TotalMilliseconds}ms");
            }

            using (var resetEvent = new ManualResetEvent(false))
            {
                resetEvent.WaitOne(timeout);
            }

            return DateTime.UtcNow;
        }
    }
}