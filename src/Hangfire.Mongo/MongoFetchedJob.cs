using System;
using Hangfire.Mongo.Database;
using Hangfire.Mongo.Dto;
using Hangfire.Storage;
using MongoDB.Driver;

namespace Hangfire.Mongo
{
    /// <summary>
    /// Hangfire fetched job for Mongo database
    /// </summary>
    public sealed class MongoFetchedJob : IFetchedJob
    {
        private readonly HangfireDbContext _connection;

        private bool _disposed;

        private bool _removedFromQueue;

        private bool _requeued;

        /// <summary>
        /// Constructs fetched job by database connection, identifier, job ID and queue
        /// </summary>
        /// <param name="connection">Database connection</param>
        /// <param name="id">Identifier</param>
        /// <param name="jobId">Job ID</param>
        /// <param name="queue">Queue name</param>
        public MongoFetchedJob(HangfireDbContext connection, int id, string jobId, string queue)
        {
            if (connection == null) throw new ArgumentNullException("connection");
            if (jobId == null) throw new ArgumentNullException("jobId");
            if (queue == null) throw new ArgumentNullException("queue");

            _connection = connection;

            Id = id;
            JobId = jobId;
            Queue = queue;
        }


        /// <summary>
        /// Identifier
        /// </summary>
        public int Id { get; private set; }

        /// <summary>
        /// Job ID
        /// </summary>
        public string JobId { get; private set; }

        /// <summary>
        /// Queue name
        /// </summary>
        public string Queue { get; private set; }

        /// <summary>
        /// Removes fetched job from a queue
        /// </summary>
        public void RemoveFromQueue()
        {
             _connection
                .JobQueue
                .DeleteOne(Builders<JobQueueDto>.Filter.Eq(_ => _.Id, Id));

            _removedFromQueue = true;
        }

        /// <summary>
        /// Puts fetched job into a queue
        /// </summary>
        public void Requeue()
        {
	        _connection.JobQueue.FindOneAndUpdate(
		        Builders<JobQueueDto>.Filter.Eq(_ => _.Id, Id),
		        Builders<JobQueueDto>.Update.Set(_ => _.FetchedAt, null));

            _requeued = true;
        }

        /// <summary>
        /// Disposes the object
        /// </summary>
        public void Dispose()
        {
            if (_disposed) return;

            if (!_removedFromQueue && !_requeued)
            {
                Requeue();
            }

            _disposed = true;
        }
    }
}