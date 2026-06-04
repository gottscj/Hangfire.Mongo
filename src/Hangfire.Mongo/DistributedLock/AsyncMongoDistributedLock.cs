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
    public class AsyncMongoDistributedLock : MongoDistributedLock
    {
        private static readonly ILog Logger = LogProvider.For<AsyncMongoDistributedLock>();

        private static readonly ThreadLocal<Dictionary<string, int>> AcquiredLocks
            = new ThreadLocal<Dictionary<string, int>>(() => new Dictionary<string, int>());


        private readonly string _resource;
        private readonly TimeSpan _timeout;
        private readonly HangfireDbContext _dbContext;
        private readonly MongoStorageOptions _storageOptions;
        private readonly string _ownerToken = Guid.NewGuid().ToString("N");

        private CancellationTokenSource _heartbeatCancellation;
        private Task _heartbeatTask;

        private int _completed;

        private readonly SemaphoreSlim _heartbeatMutex = new SemaphoreSlim(1, 1);

        /// <summary>
        /// Creates MongoDB distributed lock
        /// </summary>
        /// <param name="resource">Lock resource</param>
        /// <param name="timeout">Lock timeout</param>
        /// <param name="dbContext"></param>
        /// <param name="storageOptions">Database options</param>
        /// <exception cref="DistributedLockTimeoutException">Thrown if lock is not acquired within the timeout</exception>
        /// <exception cref="MongoDistributedLockException">Thrown if other mongo specific issue prevented the lock to be acquired</exception>
        public AsyncMongoDistributedLock(string resource,
            TimeSpan timeout,
            HangfireDbContext dbContext,
            MongoStorageOptions storageOptions) : base(resource, timeout, dbContext, storageOptions)
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
        public override IDisposable AcquireLock()
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
        public override void Dispose()
        {
            if (Interlocked.Exchange(ref _completed, 1) == 1)
            {
                return;
            }

            if (!AcquiredLocks.Value.ContainsKey(_resource))
            {
                _heartbeatMutex.Dispose();
                return;
            }

            AcquiredLocks.Value[_resource]--;

            if (AcquiredLocks.Value[_resource] > 0)
            {
                return;
            }

            AcquiredLocks.Value.Remove(_resource);

            var heartbeatTask = _heartbeatTask;
            var heartbeatCancellation = _heartbeatCancellation;
            heartbeatCancellation?.Cancel();
            var heartbeatStopped = WaitForHeartbeatToStop(heartbeatTask, heartbeatCancellation);

            var lockTaken = _heartbeatMutex.Wait(TimeSpan.FromSeconds(5));
            if (!lockTaken)
            {
                Logger.Warn($"{_resource} - heartbeat mutex did not release within 5 seconds; releasing lock without waiting for heartbeat serialization.");
            }

            try
            {
                Release();
                Cleanup();
            }
            finally
            {
                if (lockTaken)
                {
                    _heartbeatMutex.Release();
                }

                if (heartbeatStopped)
                {
                    _heartbeatMutex.Dispose();
                }
            }
        }


        /// <summary>
        /// Acquire lock
        /// </summary>
        /// <param name="timeout"></param>
        /// <exception cref="DistributedLockTimeoutException"></exception>
        /// <exception cref="MongoDistributedLockException"></exception>
        protected override void Acquire(TimeSpan timeout)
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
                            [nameof(DistributedLockDto.ExpireAt)] =
                                DateTime.UtcNow.Add(_storageOptions.DistributedLockLifetime),
                            [nameof(DistributedLockDto.OwnerToken)] = _ownerToken
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
        protected override TimeSpan CalculateTimeout(TimeSpan timeout)
        {
            return TimeSpan.FromMilliseconds((timeout.TotalMilliseconds / 1000) + 5);
        }

        /// <summary>
        /// Release the lock
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
        protected override void Cleanup()
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
        protected override void StartHeartBeat()
        {
            var timerInterval =
                TimeSpan.FromMilliseconds(_storageOptions.DistributedLockLifetime.TotalMilliseconds / 5);
            _heartbeatCancellation = new CancellationTokenSource();
            _heartbeatTask = Task.Run(() => HeartbeatLoop(timerInterval, _heartbeatCancellation.Token));
        }

        private async Task HeartbeatLoop(TimeSpan timerInterval, CancellationToken cancellationToken)
        {
            var next = DateTime.UtcNow.Add(timerInterval);
            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    var delay = next.Subtract(DateTime.UtcNow);
                    if (delay > TimeSpan.Zero)
                    {
                        await Task.Delay(delay, cancellationToken).ConfigureAwait(false);
                    }

                    await UpdateHeartbeat(cancellationToken).ConfigureAwait(false);
                    next = next.Add(timerInterval);
                    if (next <= DateTime.UtcNow)
                    {
                        next = DateTime.UtcNow.Add(timerInterval);
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

        private async Task UpdateHeartbeat(CancellationToken cancellationToken)
        {
            await _heartbeatMutex.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                if (_completed != 0) return;

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

                var result = await _dbContext.DistributedLock.UpdateOneAsync(filter, update, cancellationToken: cancellationToken)
                    .ConfigureAwait(false);
                if (result.MatchedCount == 0)
                {
                    _heartbeatCancellation?.Cancel();
                    Logger.Warn($"{_resource} - heartbeat matched 0 documents. The distributed lock may have been acquired by another owner.");
                    return;
                }

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
            finally
            {
                _heartbeatMutex.Release();
            }
        }

        private bool WaitForHeartbeatToStop(Task heartbeatTask, CancellationTokenSource heartbeatCancellation)
        {
            if (heartbeatTask == null)
            {
                heartbeatCancellation?.Dispose();
                return true;
            }

            try
            {
                if (heartbeatTask.Wait(TimeSpan.FromSeconds(5)))
                {
                    heartbeatCancellation?.Dispose();
                    return true;
                }

                Logger.Warn($"{_resource} - heartbeat did not stop within 5 seconds.");
                heartbeatTask.ContinueWith(
                    _ => heartbeatCancellation?.Dispose(),
                    CancellationToken.None,
                    TaskContinuationOptions.ExecuteSynchronously,
                    TaskScheduler.Default);
            }
            catch (AggregateException ex)
            {
                Logger.Error($"{_resource} - error waiting for heartbeat to stop. Details:\r\n{ex.Flatten()}");
                heartbeatCancellation?.Dispose();
                return true;
            }

            return false;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="resource"></param>
        /// <param name="timeout"></param>
        /// <returns></returns>
        protected override DateTime Wait(string resource, TimeSpan timeout)
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
