using System;
using System.Text.RegularExpressions;
using System.Threading;
using Hangfire.Logging;
using Hangfire.Mongo.Database;
using Hangfire.Mongo.Dto;
using MongoDB.Bson;
using MongoDB.Driver;

namespace Hangfire.Mongo.CosmosDB
{
    /// <summary>
    /// Fetches job from DB
    /// </summary>
    public class CosmosJobFetcher : MongoJobFetcher
    {
        /// <summary>
        /// ctor
        /// </summary>
        /// <param name="dbContext"></param>
        /// <param name="storageOptions"></param>
        /// <param name="semaphore"></param>
        public CosmosJobFetcher(HangfireDbContext dbContext, MongoStorageOptions storageOptions, IJobQueueSemaphore semaphore) : base(dbContext, storageOptions, semaphore)
        {
        }
        
        /// <summary>
        /// Tries to fetch a job from specified queue 
        /// </summary>
        /// <param name="queue"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public override MongoFetchedJob TryGetEnqueuedJob(string queue, CancellationToken cancellationToken)
        {
            var fetchedAtQuery = new BsonDocument(nameof(JobQueueDto.FetchedAt), BsonNull.Value);
            if(StorageOptions.InvisibilityTimeout.HasValue)
            {
                var date  =
                    DateTime.UtcNow.AddSeconds(StorageOptions.InvisibilityTimeout.Value.Negate().TotalSeconds);
                fetchedAtQuery = new BsonDocument("$or", new BsonArray
                {
                    new BsonDocument(nameof(JobQueueDto.FetchedAt), BsonNull.Value),
                    new BsonDocument(nameof(JobQueueDto.FetchedAt), new BsonDocument("$lt", date))
                });
            }
            var filter = new BsonDocument("$and", new BsonArray
            {
                new BsonDocument(nameof(JobQueueDto.Queue), queue),
                fetchedAtQuery
            });
            
            var update = new BsonDocument("$set", new BsonDocument(nameof(JobQueueDto.FetchedAt), DateTime.UtcNow));

            JobQueueDto fetchedJob;
            try
            {
                fetchedJob = DbContext
                .JobGraph
                .OfType<JobQueueDto>()
                .FindOneAndUpdate(filter, update, Options, cancellationToken);
            }
            catch (MongoCommandException ex)
            {
                var delayMs = 5000;
                var regex = new Regex(@"RetryAfterMs=(\d+),");
                var match = regex.Match(ex.Message);
                if (match.Success)
                {
                    if (!int.TryParse(match.Groups[1].Value, out delayMs))
                    {
                        delayMs = 5000;
                    }
                }

                Thread.Sleep(delayMs + 100);
                fetchedJob = DbContext
                .JobGraph
                .OfType<JobQueueDto>()
                .FindOneAndUpdate(filter, update, Options, cancellationToken);
            }

            if (fetchedJob == null)
            {
                return null;
            }
            
            if (Logger.IsTraceEnabled())
            {
                Logger.Trace($"Fetched job {fetchedJob.JobId} from '{queue}' Thread[{Thread.CurrentThread.ManagedThreadId}]");
            }
            return StorageOptions.Factory.CreateFetchedJob(DbContext, fetchedJob.Id, fetchedJob.JobId, fetchedJob.Queue);
        }
    }
}