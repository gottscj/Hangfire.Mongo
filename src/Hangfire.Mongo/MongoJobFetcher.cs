using System;
using System.Threading;
using Hangfire.Annotations;
using Hangfire.Logging;
using Hangfire.Mongo.Database;
using Hangfire.Mongo.Dto;
using Hangfire.Storage;
using MongoDB.Bson;
using MongoDB.Driver;

namespace Hangfire.Mongo
{
    /// <summary>
    /// Fetches job from DB
    /// </summary>
    public class MongoJobFetcher
    {
        private static readonly ILog Logger = LogProvider.For<MongoJobFetcher>();
        
        private readonly MongoStorageOptions _storageOptions;
        private readonly IJobQueueSemaphore _semaphore;

        private readonly HangfireDbContext _dbContext;

        private static readonly FindOneAndUpdateOptions<BsonDocument> Options = new FindOneAndUpdateOptions<BsonDocument>
        {
            IsUpsert = false,
            ReturnDocument = ReturnDocument.After
        };
        
        /// <summary>
        /// ctor
        /// </summary>
        /// <param name="dbContext"></param>
        /// <param name="storageOptions"></param>
        /// <param name="semaphore"></param>
        public MongoJobFetcher(HangfireDbContext dbContext, MongoStorageOptions storageOptions,
            IJobQueueSemaphore semaphore)
        {
            _storageOptions = storageOptions ?? throw new ArgumentNullException(nameof(storageOptions));
            _semaphore = semaphore;
            _dbContext = dbContext ?? throw new ArgumentNullException(nameof(dbContext));
        }

        /// <summary>
        /// Fetches next job, blocks until job is successfully fetched
        /// Queues are in prioritized order
        /// </summary>
        /// <param name="queues"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        /// <exception cref="ArgumentNullException"></exception>
        /// <exception cref="ArgumentException"></exception>
        [NotNull]
        public virtual IFetchedJob FetchNextJob(string[] queues, CancellationToken cancellationToken)
        {
            if (queues == null)
            {
                throw new ArgumentNullException(nameof(queues));
            }

            if (queues.Length == 0)
            {
                throw new ArgumentException("Queue array must be non-empty.", nameof(queues));
            }

            MongoFetchedJob fetchedJob = null;
            var tryAllQueues = true;
            while (fetchedJob == null)
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (tryAllQueues)
                {
                    fetchedJob = TryAllQueues(queues, cancellationToken);
                }
                
                if (fetchedJob != null)
                {
                    // make sure to try to decrement semaphore if we succeed in getting a job from the queue
                    _semaphore.WaitNonBlock(fetchedJob.Queue);
                    return fetchedJob;
                }

                if (_semaphore.WaitAny(queues, cancellationToken, _storageOptions.QueuePollInterval, out var queue, out var timedOut))
                {
                    fetchedJob = TryGetEnqueuedJob(queue, cancellationToken);
                }
                // at this point only try all queues if semaphore timed out
                tryAllQueues = timedOut;
            } 
            
            return fetchedJob;
        }

        /// <summary>
        /// Tries to get a job from db in queues defined priority
        /// </summary>
        /// <param name="queues"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public virtual MongoFetchedJob TryAllQueues(string[] queues, CancellationToken cancellationToken)
        {
            if (Logger.IsTraceEnabled())
            {
                Logger.Trace($"Try fetch from queues: {string.Join(",", queues)} Thread[{Thread.CurrentThread.ManagedThreadId}]");
            }
            
            foreach (var queue in queues)
            {
                var fetchedJob = TryGetEnqueuedJob(queue, cancellationToken);
                if (fetchedJob == null)
                {
                    continue;
                }
                return fetchedJob;
            }

            return null;
        }

        /// <summary>
        /// Tries to fetch a job from specified queue 
        /// </summary>
        /// <param name="queue"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public virtual MongoFetchedJob TryGetEnqueuedJob(string queue, CancellationToken cancellationToken)
        {
            var fetchedAtQuery = new BsonDocument(nameof(JobDto.FetchedAt), BsonNull.Value);
            if(_storageOptions.SlidingInvisibilityTimeout.HasValue)
            {
                var date  =
                    DateTime.UtcNow.AddSeconds(_storageOptions.SlidingInvisibilityTimeout.Value.Negate().TotalSeconds);
                fetchedAtQuery = new BsonDocument("$or", new BsonArray
                {
                    new BsonDocument(nameof(JobDto.FetchedAt), BsonNull.Value),
                    new BsonDocument(nameof(JobDto.FetchedAt), new BsonDocument("$lt", date))
                });
            }
            var filter = new BsonDocument("$and", new BsonArray
            {
                new BsonDocument(nameof(JobDto.Queue), queue),
                new BsonDocument("_t", nameof(JobDto)),
                fetchedAtQuery
            });
            var fetchedAt = DateTime.UtcNow;
            var update = new BsonDocument("$set", new BsonDocument(nameof(JobDto.FetchedAt), fetchedAt));
            
            var fetchedJobDoc = _dbContext
                .JobGraph
                .FindOneAndUpdate(filter, update, Options, cancellationToken);
            
            if (fetchedJobDoc == null)
            {
                return null;
            }

            var fetchedJob = new JobDto(fetchedJobDoc);
            if (Logger.IsTraceEnabled())
            {
                Logger.Trace($"Fetched job {fetchedJob.Id} from '{queue}' Thread[{Thread.CurrentThread.ManagedThreadId}]");
            }
            return _storageOptions.Factory.CreateFetchedJob(_dbContext, _storageOptions, fetchedAt, fetchedJob.Id, fetchedJob.Id, fetchedJob.Queue);
        }
    }
}