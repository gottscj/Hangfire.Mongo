using System;
using System.Text.RegularExpressions;
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
#pragma warning disable 1591
    internal class MongoJobFetcher
    {
        private static readonly ILog Logger = LogProvider.For<MongoJobFetcher>();
        
        private readonly MongoStorageOptions _storageOptions;
        private readonly IJobQueueSemaphore _semaphore;

        private readonly HangfireDbContext _dbContext;

        private static readonly FindOneAndUpdateOptions<JobQueueDto> Options = new FindOneAndUpdateOptions<JobQueueDto>
        {
            IsUpsert = false,
            ReturnDocument = ReturnDocument.After
        };
        
        public MongoJobFetcher(HangfireDbContext dbContext, MongoStorageOptions storageOptions,
            IJobQueueSemaphore semaphore)
        {
            _storageOptions = storageOptions ?? throw new ArgumentNullException(nameof(storageOptions));
            _semaphore = semaphore;
            _dbContext = dbContext ?? throw new ArgumentNullException(nameof(dbContext));
        }

        [NotNull]
        public IFetchedJob FetchNextJob(string[] queues, CancellationToken cancellationToken)
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

        private MongoFetchedJob TryAllQueues(string[] queues, CancellationToken cancellationToken)
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

        private MongoFetchedJob TryGetEnqueuedJob(string queue, CancellationToken cancellationToken)
        {
            var fetchedAtQuery = new BsonDocument(nameof(JobQueueDto.FetchedAt), BsonNull.Value);
            if(_storageOptions.InvisibilityTimeout.HasValue)
            {
                var date  =
                    DateTime.UtcNow.AddSeconds(_storageOptions.InvisibilityTimeout.Value.Negate().TotalSeconds);
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
                fetchedJob = _dbContext
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
                fetchedJob = _dbContext
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
            return new MongoFetchedJob(_dbContext, fetchedJob.Id, fetchedJob.JobId, fetchedJob.Queue);

        }
    }
#pragma warning disable 1591
}