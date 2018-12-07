using System;
using System.Threading;
using Hangfire.Annotations;
using Hangfire.Mongo.Database;
using Hangfire.Mongo.Dto;
using Hangfire.Storage;
using MongoDB.Driver;

namespace Hangfire.Mongo
{
#pragma warning disable 1591
    internal sealed class MongoJobQueue : IDisposable
    {
        private readonly MongoStorageOptions _storageOptions;

        private readonly HangfireDbContext _dbContext;

        public MongoJobQueue(HangfireDbContext dbContext, MongoStorageOptions storageOptions)
        {
            _storageOptions = storageOptions ?? throw new ArgumentNullException(nameof(storageOptions));
            _dbContext = dbContext ?? throw new ArgumentNullException(nameof(dbContext));
        }

        [NotNull]
        public IFetchedJob Dequeue(string[] queues, CancellationToken cancellationToken)
        {
            if (queues == null)
            {
                throw new ArgumentNullException(nameof(queues));
            }

            if (queues.Length == 0)
            {
                throw new ArgumentException("Queue array must be non-empty.", nameof(queues));
            }


            var filter = Builders<JobQueueDto>.Filter;
            var fetchConditions = new[]
            {
                filter.Eq(_ => _.FetchedAt, null),
                filter.Lt(_ => _.FetchedAt, DateTime.UtcNow.AddSeconds(_storageOptions.InvisibilityTimeout.Negate().TotalSeconds))
            };
            var fetchConditionsIndex = 0;

            var options = new FindOneAndUpdateOptions<JobQueueDto>
            {
                IsUpsert = false,
                ReturnDocument = ReturnDocument.After
            };

            JobQueueDto fetchedJob = null;
            while (fetchedJob == null)
            {
                cancellationToken.ThrowIfCancellationRequested();

                var fetchCondition = fetchConditions[fetchConditionsIndex];

                foreach (var queue in queues)
                {
                    fetchedJob = _dbContext
                        .JobGraph
                        .OfType<JobQueueDto>()
                        .FindOneAndUpdate(
                            fetchCondition & filter.Eq(_ => _.Queue, queue),
                            Builders<JobQueueDto>.Update.Set(_ => _.FetchedAt, DateTime.UtcNow),
                            options,
                            cancellationToken);
                    if (fetchedJob != null)
                    {
                        break;
                    }
                }

                if (fetchedJob == null)
                {
                    // No more jobs found in any of the requested queues...
                    if (fetchConditionsIndex == fetchConditions.Length - 1)
                    {
                        // ...and we are out of fetch conditions as well.
                        // Wait for a while before polling again.
                        cancellationToken.WaitHandle.WaitOne(_storageOptions.QueuePollInterval);
                        cancellationToken.ThrowIfCancellationRequested();
                    }
                }

                // Move on to next fetch condition
                fetchConditionsIndex = (fetchConditionsIndex + 1) % fetchConditions.Length;
            }

            return new MongoFetchedJob(_dbContext, fetchedJob.Id, fetchedJob.JobId, fetchedJob.Queue);
        }

        public void Dispose()
        {
            // nop
        }
    }
#pragma warning disable 1591
}