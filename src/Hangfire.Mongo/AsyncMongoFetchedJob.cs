using System;
using System.Threading;
using System.Threading.Tasks;
using Hangfire.Logging;
using Hangfire.Mongo.Database;
using Hangfire.Mongo.Dto;
using Hangfire.States;
using MongoDB.Bson;
using MongoDB.Driver;

namespace Hangfire.Mongo
{
    /// <summary>
    /// Fetched job which keeps its lease alive with an asynchronous heartbeat instead of a
    /// timer callback running a synchronous database call on a thread-pool thread.
    /// The heartbeat only updates the job while this instance still owns the lease (FetchToken),
    /// so it never extends a lease that was reclaimed by another worker.
    /// </summary>
    /// <remarks>
    /// <see cref="MongoFetchedJob.FetchedAt"/> keeps the value from when the job was fetched.
    /// </remarks>
    public class AsyncMongoFetchedJob : MongoFetchedJob
    {
        private static readonly ILog Logger = LogProvider.For<AsyncMongoFetchedJob>();

        private readonly HangfireDbContext _db;
        private readonly AsyncHeartbeat _heartbeat;

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
        public AsyncMongoFetchedJob(
            HangfireDbContext db,
            MongoStorageOptions storageOptions,
            DateTime fetchedAt,
            string fetchToken,
            ObjectId id,
            ObjectId jobId,
            string queue)
            : base(db, storageOptions, fetchedAt, fetchToken, id, jobId, queue)
        {
            _db = db;
            if (storageOptions.SlidingInvisibilityTimeout is { } timeout && timeout > TimeSpan.Zero)
            {
                var interval = TimeSpan.FromTicks(timeout.Ticks / 5);
                _heartbeat = AsyncHeartbeat.Start(interval, UpdateFetchedAtAsync,
                    ex => Logger.Error($"Job: {Id} - Unable to update heartbeat. Details:\r\n{ex}"));
            }
        }

        /// <summary>
        /// Disposes the object
        /// </summary>
        public override void Dispose()
        {
            _heartbeat?.Stop();
            base.Dispose();
        }

        /// <summary>
        /// Intentionally empty: the base timer is replaced by the asynchronous heartbeat started in the constructor.
        /// </summary>
        /// <param name="slidingInvisibilityTimeout"></param>
        protected override void StartHeartbeat(TimeSpan slidingInvisibilityTimeout)
        {
        }

        private async Task UpdateFetchedAtAsync(CancellationToken cancellationToken)
        {
            var filter = new BsonDocument
            {
                ["_id"] = Id,
                [nameof(JobDto.FetchToken)] = FetchToken,
                [nameof(JobDto.StateName)] = ProcessingState.StateName
            };
            var update = new BsonDocument
            {
                ["$set"] = new BsonDocument
                {
                    [nameof(JobDto.FetchedAt)] = DateTime.UtcNow
                }
            };
            await _db.JobGraph.UpdateOneAsync(filter, update, cancellationToken: cancellationToken)
                .ConfigureAwait(false);
        }
    }
}
