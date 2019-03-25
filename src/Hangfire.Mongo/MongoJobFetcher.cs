using System;
using System.Linq;
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
        private readonly IJobQueueSemaphore _jobQueueSemaphore;

        private readonly HangfireDbContext _dbContext;

        private readonly DateTime _invisibilityTimeout;
        
        private static readonly FindOneAndUpdateOptions<JobQueueDto> Options = new FindOneAndUpdateOptions<JobQueueDto>
        {
            IsUpsert = false,
            ReturnDocument = ReturnDocument.After
        };
        
        public MongoJobFetcher(HangfireDbContext dbContext, MongoStorageOptions storageOptions,
            IJobQueueSemaphore jobQueueSemaphore)
        {
            _storageOptions = storageOptions ?? throw new ArgumentNullException(nameof(storageOptions));
            _jobQueueSemaphore = jobQueueSemaphore;
            _dbContext = dbContext ?? throw new ArgumentNullException(nameof(dbContext));
            
            _invisibilityTimeout =
                DateTime.UtcNow.AddSeconds(_storageOptions.InvisibilityTimeout.Negate().TotalSeconds);
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

            cancellationToken.ThrowIfCancellationRequested();

            // try all queues
            var fetchedJob = TryAllQueues(queues, cancellationToken);

            if (fetchedJob != null)
            {
                return fetchedJob;
            }

            // no luck wait for signal
            fetchedJob = WaitForJobToBeEnqueued(queues, cancellationToken);
            return fetchedJob;
        }

        private MongoFetchedJob WaitForJobToBeEnqueued(string[] queues, CancellationToken cancellationToken)
        {
            MongoFetchedJob fetchedJob;
            
            do
            {
                // no result wait for signal or poll time out
                var queueIndex = _jobQueueSemaphore
                    .WaitAny(queues, cancellationToken, _storageOptions.QueuePollInterval); 
                    
                if (queueIndex == WaitHandle.WaitTimeout)
                {
                    // waithandle's timed out, poll all queues
                    fetchedJob = TryAllQueues(queues, cancellationToken);
                }
                else
                {
                    var queue = queues[queueIndex];
                    fetchedJob = GetEnqueuedJob(queue, cancellationToken);
                }
                
            } while (fetchedJob == null);

            _jobQueueSemaphore.Release(fetchedJob.Queue);
            return fetchedJob;
        }

        private MongoFetchedJob TryAllQueues(string[] queues, CancellationToken cancellationToken)
        {
            return queues
                .Select(queue => GetEnqueuedJob(queue, cancellationToken))
                .FirstOrDefault(j => j != null);
        }

        private MongoFetchedJob GetEnqueuedJob(string queue, CancellationToken cancellationToken)
        {
            var filter = new BsonDocument("$and", new BsonArray
            {
                new BsonDocument(nameof(JobQueueDto.Queue), queue),
                new BsonDocument("$or", new BsonArray
                {
                    new BsonDocument(nameof(JobQueueDto.FetchedAt), BsonNull.Value),
                    new BsonDocument(nameof(JobQueueDto.FetchedAt), new BsonDocument("$lt", _invisibilityTimeout))
                })
                
            });
            var update = new BsonDocument("$set", new BsonDocument(nameof(JobQueueDto.FetchedAt), DateTime.UtcNow));
            
            var fetchedJob = _dbContext
                .JobGraph
                .OfType<JobQueueDto>()
                .FindOneAndUpdate(filter, update, Options, cancellationToken);

            if (fetchedJob == null)
            {
                return null;
            }
            
            Logger.Debug($"job:{fetchedJob.JobId}:{fetchedJob.Queue} - [{Thread.CurrentThread.ManagedThreadId}] - start");
            return new MongoFetchedJob(_dbContext, fetchedJob.Id, fetchedJob.JobId, fetchedJob.Queue);

        }
    }
#pragma warning disable 1591
}