using System;
using System.Threading;
using Hangfire.Logging;
using Hangfire.Mongo.Database;
using Hangfire.Mongo.Dto;
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
        private readonly object _syncRoot = new object();
        private static readonly ILog Logger = LogProvider.For<MongoFetchedJob>();
        private readonly HangfireDbContext _db;
        private readonly MongoStorageOptions _storageOptions;
        private readonly DateTime _fetchedAt;
        private readonly ObjectId _id;

        private bool _disposed;

        private bool _removedFromQueue;

        private bool _requeued;

        /// <summary>
        /// Constructs fetched job by database connection, identifier, job ID and queue
        /// </summary>
        /// <param name="db">Database connection</param>
        /// <param name="storageOptions">storage options</param>
        /// <param name="fetchedAt"></param>
        /// <param name="id">Identifier</param>
        /// <param name="jobId">Job ID</param>
        /// <param name="queue">Queue name</param>
        public MongoFetchedJob(
            HangfireDbContext db, 
            MongoStorageOptions storageOptions, 
            DateTime fetchedAt,
            ObjectId id, 
            ObjectId jobId, 
            string queue)
        {
            _db = db ?? throw new ArgumentNullException(nameof(db));
            _storageOptions = storageOptions;
            _fetchedAt = fetchedAt;
            _id = id;
            JobId = jobId.ToString();
            Queue = queue ?? throw new ArgumentNullException(nameof(queue));
        }

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
                var filter = new BsonDocument
                {
                    ["_id"] = _id,
                    [nameof(JobQueueDto.FetchedAt)] = BsonValue.Create(_fetchedAt),
                    [nameof(JobQueueDto.Queue)] = Queue
                };
                var result = _db.JobGraph.OfType<JobQueueDto>().DeleteOne(filter);
                
                if (Logger.IsTraceEnabled())
                {
                    if (result.DeletedCount > 0)
                    {
                        Logger.Trace($"Remove job '{JobId}' from queue '{Queue}'");
                    }
                    else
                    {
                        Logger.Trace($"Did not remove job '{JobId}' from queue '{Queue}', could be invisibility timeout is exceeded");
                    }
                }
                _removedFromQueue = true;
            }
        }

        /// <summary>
        /// Puts fetched job into a queue
        /// </summary>
        public virtual void Requeue()
        {
            lock (_syncRoot)
            {
                _db.JobGraph.OfType<JobQueueDto>().FindOneAndUpdate(
                    Builders<JobQueueDto>.Filter.Eq(_ => _.Id, _id),
                    Builders<JobQueueDto>.Update.Set(_ => _.FetchedAt, null));
                
                if (_storageOptions.CheckQueuedJobsStrategy == CheckQueuedJobsStrategy.TailNotificationsCollection)
                {
                    _db.Notifications.InsertOne(NotificationDto.JobEnqueued(Queue), new InsertOneOptions
                    {
                        BypassDocumentValidation = false
                    });    
                }
                
                if (Logger.IsTraceEnabled())
                {
                    Logger.Trace($"Requeue job '{JobId}' from queue '{Queue}'");
                }
                _requeued = true;
            }
        }

        /// <summary>
        /// Disposes the object
        /// </summary>
        public virtual void Dispose()
        {
            if (_disposed) return;
            lock (_syncRoot)
            {
                if (!_removedFromQueue && !_requeued)
                {
                    Requeue();
                }
            }
            

            _disposed = true;
        }
    }
}