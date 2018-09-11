using System;
using System.Threading;
using Hangfire.Annotations;
using Hangfire.Mongo.Database;
using Hangfire.Mongo.Dto;
using Hangfire.Storage;
using MongoDB.Bson;
using MongoDB.Driver;

namespace Hangfire.Mongo.PersistentJobQueue.Mongo
{
#pragma warning disable 1591
    internal class MongoJobQueue : IPersistentJobQueue
    {
        private readonly MongoStorageOptions _storageOptions;

        private readonly HangfireDbContext _connection;

        public MongoJobQueue(HangfireDbContext connection, MongoStorageOptions storageOptions)
        {
            _storageOptions = storageOptions ?? throw new ArgumentNullException(nameof(storageOptions));
            _connection = connection ?? throw new ArgumentNullException(nameof(connection));
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
                    fetchedJob = _connection
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

            return new MongoFetchedJob(_connection, fetchedJob.Id, fetchedJob.JobId, fetchedJob.Queue);
        }

        public void Enqueue(string queue, string jobId)
        {
            _connection.JobGraph.InsertOne(new JobQueueDto
            {
                JobId = ObjectId.Parse(jobId),
                Queue = queue,
                Id = ObjectId.GenerateNewId(),
                FetchedAt = null
            });
        }
    }
#pragma warning disable 1591
}