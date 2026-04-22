using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using System.Threading;
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
        private readonly object _syncRoot;

        private bool _disposed;
        private bool _removedFromQueue;
        private bool _requeued;
        private Timer _heartbeatTimer;


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
            _syncRoot = new object();
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
            lock (_syncRoot)
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
            _removedFromQueue = true;
        }

        /// <summary>
        /// Puts fetched job into a queue
        /// </summary>
        public virtual void Requeue()
        {
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
            lock (_syncRoot)
            {
                if (_disposed) return;

                _heartbeatTimer?.Dispose();
                _heartbeatTimer = null;

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
            
            var filter = new BsonDocument
            {
                ["_id"] = _id,
                [nameof(JobDto.StateName)] = ProcessingState.StateName
            };
            _heartbeatTimer = new Timer(_ =>
            {
                // Timer callback may be invoked after the Dispose method call,
                // so we are using lock to avoid un synchronized calls.
                lock (_syncRoot)
                {
                    if (_disposed)
                    {
                        return;
                    }

                    if (_requeued || _removedFromQueue)
                    {
                        return;
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
                        _db.JobGraph.UpdateOne(filter, update);
                        _fetchedAt = now;
                        
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
                    catch (Exception ex)
                    {
                        Logger.Error($"Job: {Id} - Unable to update heartbeat. Details:\r\n{ex}");
                    }
                }
            }, null, timerInterval, timerInterval);
        }
    }
}