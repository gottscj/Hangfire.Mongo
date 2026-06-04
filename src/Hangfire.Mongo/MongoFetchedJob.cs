using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Hangfire.Logging;
using Hangfire.Mongo.Database;
using Hangfire.Mongo.Dto;
using Hangfire.States;
using Hangfire.Storage;
using MongoDB.Bson;
using MongoDB.Driver;

namespace Hangfire.Mongo
{
    /// <summary>
    /// Hangfire fetched job for Mongo database
    /// </summary>
    public class MongoFetchedJob : IFetchedJob
    {
        private static readonly ILog Logger = LogProvider.For<MongoFetchedJob>();

        private readonly HangfireDbContext _db;
        private readonly MongoStorageOptions _storageOptions;
        private DateTime _fetchedAt;
        private readonly string _fetchToken;
        private readonly ObjectId _id;
        private readonly SemaphoreSlim _syncRoot;

        private bool _disposed;
        private bool _removedFromQueue;
        private bool _requeued;
        private bool _leaseLost;
        private CancellationTokenSource _heartbeatCancellation;
        private Task _heartbeatTask;


        /// <summary>
        /// Constructs fetched job by database connection, identifier, job ID and queue
        /// </summary>
        /// <param name="db">Database connection</param>
        /// <param name="storageOptions">storage options</param>
        /// <param name="fetchedAt"></param>
        /// <param name="fetchToken"></param>
        /// <param name="id">Identifier</param>
        /// <param name="jobId">Job ID</param>
        /// <param name="queue">Queue name</param>
        public MongoFetchedJob(
            HangfireDbContext db,
            MongoStorageOptions storageOptions,
            DateTime fetchedAt,
            string fetchToken,
            ObjectId id,
            ObjectId jobId,
            string queue)
        {
            _db = db ?? throw new ArgumentNullException(nameof(db));
            _syncRoot = new SemaphoreSlim(1, 1);
            _storageOptions = storageOptions;
            _fetchedAt = fetchedAt;
            _fetchToken = fetchToken ?? throw new ArgumentNullException(nameof(fetchToken));
            _id = id;
            JobId = jobId.ToString();
            Queue = queue ?? throw new ArgumentNullException(nameof(queue));
            if (storageOptions.SlidingInvisibilityTimeout.HasValue)
            {
                StartHeartbeat(storageOptions.SlidingInvisibilityTimeout.Value);
            }
        }

        /// <summary>
        /// Immutable ownership token issued at fetch
        /// </summary>
        public string FetchToken => _fetchToken;

        /// <summary>
        /// Timestamp job is fetched
        /// </summary>
        public DateTime FetchedAt => _fetchedAt;

        /// <summary>
        /// Id of job
        /// </summary>
        public ObjectId Id => _id;

        /// <summary>
        /// Job ID
        /// </summary>
        public string JobId { get; }

        /// <summary>
        /// Queue name
        /// </summary>
        public string Queue { get; }

        /// <summary>
        /// Removes fetched job from a queue
        /// </summary>
        public virtual void RemoveFromQueue()
        {
            _syncRoot.Wait();
            try
            {
                if (_removedFromQueue || _requeued)
                {
                    return;
                }

                var filter = new BsonDocument
                {
                    ["_id"] = _id,
                    [nameof(JobDto.FetchToken)] = _fetchToken,
                    [nameof(JobDto.Queue)] = Queue
                };
                var update = new BsonDocument
                {
                    ["$set"] = new BsonDocument
                    {
                        [nameof(JobDto.FetchedAt)] = BsonNull.Value,
                        [nameof(JobDto.FetchToken)] = BsonNull.Value,
                        [nameof(JobDto.Queue)] = BsonNull.Value
                    }
                };
                var result = _db.JobGraph.UpdateOne(filter, update);
                _removedFromQueue = true;
                CancelHeartbeat();
                if (result.MatchedCount == 0)
                {
                    _leaseLost = true;
                    // Ack lost: either this lease was stolen by another worker after our
                    // invisibility timeout, or the document is already gone. Log and return —
                    // surfacing this as an exception would push Hangfire.Core's Worker into a
                    // retry path and, ironically, risk double-delivery.
                    Logger.Warn(
                        $"Lease lost for job {_id} (queue='{Queue}'): queue acknowledgement matched 0 documents. " +
                        "Another worker may have reclaimed this job.");
                }
            }
            finally
            {
                _syncRoot.Release();
            }
        }

        /// <summary>
        /// Sets internal parameter to indicate if job is removed from queue
        /// </summary>
        public virtual void SetRemoved()
        {
            _syncRoot.Wait();
            try
            {
                _removedFromQueue = true;
                CancelHeartbeat();
            }
            finally
            {
                _syncRoot.Release();
            }
        }

        /// <summary>
        /// Puts fetched job into a queue
        /// </summary>
        public virtual void Requeue()
        {
            _syncRoot.Wait();
            try
            {
                if (_removedFromQueue || _requeued)
                {
                    return;
                }

                var filter = new BsonDocument
                {
                    ["_id"] = _id,
                    [nameof(JobDto.FetchToken)] = _fetchToken,
                    [nameof(JobDto.Queue)] = Queue
                };
                var update = new BsonDocument
                {
                    ["$set"] = new BsonDocument
                    {
                        [nameof(JobDto.FetchedAt)] = BsonNull.Value,
                        [nameof(JobDto.FetchToken)] = BsonNull.Value,
                        [nameof(JobDto.Queue)] = Queue.ToBsonValue()
                    }
                };

                var result = _db.JobGraph.UpdateOne(filter, update);
                if (result.MatchedCount == 0)
                {
                    _leaseLost = true;
                    CancelHeartbeat();
                    Logger.Warn(
                        $"Lease lost for job {_id} (queue='{Queue}'): requeue matched 0 documents. " +
                        "Another worker may have reclaimed this job.");
                    return;
                }

                _requeued = true;
                CancelHeartbeat();
                SignalRequeuedJob();
            }
            finally
            {
                _syncRoot.Release();
            }
        }

        /// <summary>
        /// Disposes the object
        /// </summary>
        public virtual void Dispose()
        {
            CancelHeartbeat();

            Task heartbeatTask;
            CancellationTokenSource heartbeatCancellation;
            bool shouldRequeue;

            _syncRoot.Wait();
            try
            {
                if (_disposed) return;

                _disposed = true;
                shouldRequeue = !_removedFromQueue && !_requeued;
                heartbeatTask = _heartbeatTask;
                heartbeatCancellation = _heartbeatCancellation;
                _heartbeatTask = null;
                _heartbeatCancellation = null;
            }
            finally
            {
                _syncRoot.Release();
            }

            var heartbeatStopped = WaitForHeartbeatToStop(heartbeatTask, heartbeatCancellation);

            try
            {
                if (shouldRequeue)
                {
                    // will create a new instance of MongoFetchedJob
                    Requeue();
                }
            }
            finally
            {
                if (heartbeatStopped)
                {
                    _syncRoot.Dispose();
                }
            }
        }

        private void StartHeartbeat(TimeSpan slidingInvisibilityTimeout)
        {
            var timerInterval = TimeSpan.FromSeconds(slidingInvisibilityTimeout.TotalSeconds / 5);

            var filter = new BsonDocument
            {
                ["_id"] = _id,
                [nameof(JobDto.FetchToken)] = _fetchToken,
                [nameof(JobDto.Queue)] = Queue,
                [nameof(JobDto.StateName)] = ProcessingState.StateName
            };
            _heartbeatCancellation = new CancellationTokenSource();
            _heartbeatTask = Task.Run(() => HeartbeatLoop(timerInterval, filter, _heartbeatCancellation.Token));
        }

        private async Task HeartbeatLoop(TimeSpan timerInterval, BsonDocument filter, CancellationToken cancellationToken)
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

                    await UpdateHeartbeat(filter, cancellationToken).ConfigureAwait(false);
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
                    Logger.Error($"Job: {Id} - Unable to update heartbeat. Details:\r\n{ex}");
                }
            }
        }

        private void CancelHeartbeat()
        {
            try
            {
                _heartbeatCancellation?.Cancel();
            }
            catch (ObjectDisposedException)
            {
            }
        }

        private async Task UpdateHeartbeat(BsonDocument filter, CancellationToken cancellationToken)
        {
            await _syncRoot.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                if (_disposed || _requeued || _removedFromQueue || _leaseLost)
                {
                    return;
                }
            }
            finally
            {
                _syncRoot.Release();
            }

            Stopwatch sw = null;
            if (Logger.IsTraceEnabled())
            {
                sw = Stopwatch.StartNew();
            }

            var now = DateTime.UtcNow;
            var update = new BsonDocument
            {
                ["$set"] = new BsonDocument
                {
                    [nameof(JobDto.FetchedAt)] = now
                }
            };
            var result = await _db.JobGraph.UpdateOneAsync(filter, update, cancellationToken: cancellationToken)
                .ConfigureAwait(false);

            await _syncRoot.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                if (_disposed || _requeued || _removedFromQueue || _leaseLost)
                {
                    return;
                }

                if (result.MatchedCount == 0)
                {
                    _leaseLost = true;
                    CancelHeartbeat();
                    Logger.Warn(
                        $"Job: {Id} - heartbeat matched 0 documents for queue='{Queue}'. " +
                        "The fetched job lease may have been reclaimed by another worker.");
                    return;
                }

                _fetchedAt = now;
            }
            finally
            {
                _syncRoot.Release();
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
                builder.AppendLine($"Job heartbeat");
                builder.AppendLine($"{serializedModel.ToJson()}");
                builder.AppendLine($"Executed in {sw.ElapsedMilliseconds} ms");
                Logger.Trace($"{builder}");
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

                Logger.Warn($"Job: {Id} - heartbeat did not stop within 5 seconds.");
                heartbeatTask.ContinueWith(
                    _ => heartbeatCancellation?.Dispose(),
                    CancellationToken.None,
                    TaskContinuationOptions.ExecuteSynchronously,
                    TaskScheduler.Default);
            }
            catch (AggregateException ex)
            {
                Logger.Error($"Job: {Id} - error waiting for heartbeat to stop. Details:\r\n{ex.Flatten()}");
                heartbeatCancellation?.Dispose();
                return true;
            }

            return false;
        }

        private void SignalRequeuedJob()
        {
            if (_storageOptions.CheckQueuedJobsStrategy != CheckQueuedJobsStrategy.TailNotificationsCollection)
            {
                return;
            }

            _db.Notifications.InsertOne(
                NotificationDto.JobEnqueued(Queue).Serialize(),
                new InsertOneOptions
                {
                    BypassDocumentValidation = false
                });
        }
    }
}
