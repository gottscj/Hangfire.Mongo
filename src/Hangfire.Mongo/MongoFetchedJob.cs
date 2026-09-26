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
        private readonly string _ownerToken;
        private readonly ObjectId _id;
        private readonly object _syncRoot;

        private bool _disposed;
        private bool _removedFromQueue;
        private bool _requeued;
        private CancellationTokenSource _heartbeatCancellation;
        private Task _heartbeatTask;


        /// <summary>
        /// Constructs fetched job by database connection, identifier, job ID and queue
        /// </summary>
        /// <param name="db">Database connection</param>
        /// <param name="storageOptions">storage options</param>
        /// <param name="fetchedAt"></param>
        /// <param name="ownerToken"></param>
        /// <param name="id">Identifier</param>
        /// <param name="jobId">Job ID</param>
        /// <param name="queue">Queue name</param>
        public MongoFetchedJob(
            HangfireDbContext db,
            MongoStorageOptions storageOptions,
            DateTime fetchedAt,
            string ownerToken,
            ObjectId id,
            ObjectId jobId,
            string queue)
        {
            _db = db ?? throw new ArgumentNullException(nameof(db));
            _syncRoot = new object();
            _storageOptions = storageOptions;
            _fetchedAt = fetchedAt;
            _ownerToken = ownerToken ?? throw new ArgumentNullException(nameof(ownerToken));
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
        public string OwnerToken => _ownerToken;

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
            StopHeartbeat();

            lock (_syncRoot)
            {
                if (_removedFromQueue || _requeued)
                {
                    return;
                }

                var filter = new BsonDocument
                {
                    ["_id"] = _id,
                    [nameof(JobDto.OwnerToken)] = _ownerToken,
                    [nameof(JobDto.Queue)] = Queue
                };
                var update = new BsonDocument
                {
                    ["$set"] = new BsonDocument
                    {
                        [nameof(JobDto.FetchedAt)] = BsonNull.Value,
                        [nameof(JobDto.OwnerToken)] = BsonNull.Value,
                        [nameof(JobDto.Queue)] = BsonNull.Value
                    }
                };
                var result = _db.JobGraph.UpdateOne(filter, update);
                _removedFromQueue = true;
                if (result.ModifiedCount == 0)
                {
                    // Ack lost: either this lease was stolen by another worker after our
                    // invisibility timeout, or the document is already gone. Log and return —
                    // surfacing this as an exception would push Hangfire.Core's Worker into a
                    // retry path and, ironically, risk double-delivery.
                    Logger.Warn(
                        $"Lease lost for job {_id} (queue='{Queue}'): queue acknowledgement modified 0 documents. " +
                        "Another worker may have reclaimed this job.");
                }
            }
        }

        /// <summary>
        /// Sets internal parameter to indicate if job is removed from queue
        /// </summary>
        public virtual void SetRemoved()
        {
            StopHeartbeat();

            lock (_syncRoot)
            {
                _removedFromQueue = true;
            }
        }

        /// <summary>
        /// Puts fetched job into a queue
        /// </summary>
        public virtual void Requeue()
        {
            StopHeartbeat();

            lock (_syncRoot)
            {
                if (_removedFromQueue || _requeued)
                {
                    return;
                }

                using var t = _storageOptions.Factory.CreateMongoWriteOnlyTransaction(_db, _storageOptions);
                t.Requeue(_id, Queue);
                t.Commit();
                _requeued = true;
            }
        }

        /// <summary>
        /// Disposes the object
        /// </summary>
        public virtual void Dispose()
        {
            StopHeartbeat();

            lock (_syncRoot)
            {
                if (_disposed) return;

                _disposed = true;
            }

            if (!_removedFromQueue && !_requeued)
            {
                // will create a new instance of MongoFetchedJob
                Requeue();
            }
        }

        private void StartHeartbeat(TimeSpan slidingInvisibilityTimeout)
        {
            var timerInterval = TimeSpan.FromSeconds(slidingInvisibilityTimeout.TotalSeconds / 5);
            _heartbeatCancellation = new CancellationTokenSource();
            _heartbeatTask = RunHeartbeatAsync(timerInterval, _heartbeatCancellation.Token);
        }

        private async Task RunHeartbeatAsync(TimeSpan timerInterval, CancellationToken cancellationToken)
        {
            var filter = new BsonDocument
            {
                ["_id"] = _id,
                [nameof(JobDto.OwnerToken)] = _ownerToken,
                [nameof(JobDto.StateName)] = ProcessingState.StateName
            };

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

                lock (_syncRoot)
                {
                    if (_disposed || _requeued || _removedFromQueue)
                    {
                        return;
                    }
                }

                Stopwatch sw = null;
                if (Logger.IsTraceEnabled())
                {
                    sw = Stopwatch.StartNew();
                }

                try
                {
                    var now = DateTime.UtcNow;
                    var update = new BsonDocument
                    {
                        ["$set"] = new BsonDocument
                        {
                            [nameof(JobDto.FetchedAt)] = now
                        }
                    };
                    await _db.JobGraph
                        .UpdateOneAsync(filter, update, cancellationToken: cancellationToken)
                        .ConfigureAwait(false);

                    lock (_syncRoot)
                    {
                        if (!_disposed && !_requeued && !_removedFromQueue)
                        {
                            _fetchedAt = now;
                        }
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

        private void StopHeartbeat()
        {
            CancellationTokenSource cancellation;
            Task heartbeat;
            lock (_syncRoot)
            {
                cancellation = _heartbeatCancellation;
                heartbeat = _heartbeatTask;
                cancellation?.Cancel();
            }

            heartbeat?.GetAwaiter().GetResult();

            lock (_syncRoot)
            {
                if (ReferenceEquals(_heartbeatTask, heartbeat))
                {
                    _heartbeatCancellation = null;
                    _heartbeatTask = null;
                    cancellation?.Dispose();
                }
            }
        }
    }
}