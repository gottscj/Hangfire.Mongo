using System;
using System.Globalization;
using System.Threading;
using Hangfire.Annotations;
using Hangfire.Mongo.Database;
using Hangfire.Mongo.Dto;
using Hangfire.Mongo.MongoUtils;
using Hangfire.Storage;
using MongoDB.Driver;

namespace Hangfire.Mongo.PersistentJobQueue.Mongo
{
#pragma warning disable 1591
	public class MongoJobQueue : IPersistentJobQueue
    {
        private readonly MongoStorageOptions _options;

        private readonly HangfireDbContext _connection;

        public MongoJobQueue(HangfireDbContext connection, MongoStorageOptions options)
        {
            if (options == null)
                throw new ArgumentNullException("options");

            if (connection == null)
                throw new ArgumentNullException("connection");

            _options = options;
            _connection = connection;
        }

        [NotNull]
        public IFetchedJob Dequeue(string[] queues, CancellationToken cancellationToken)
        {
            if (queues == null)
                throw new ArgumentNullException("queues");

            if (queues.Length == 0)
                throw new ArgumentException("Queue array must be non-empty.", "queues");

            JobQueueDto fetchedJob = null;

            var fetchConditions = new[]
			{
				Builders<JobQueueDto>.Filter.Eq(_ => _.FetchedAt, null),
				Builders<JobQueueDto>.Filter.Lt(_ => _.FetchedAt, _connection.GetServerTimeUtc().AddSeconds(_options.InvisibilityTimeout.Negate().TotalSeconds))
			};
            var currentQueryIndex = 0;

            do
            {
                cancellationToken.ThrowIfCancellationRequested();

                FilterDefinition<JobQueueDto> fetchCondition = fetchConditions[currentQueryIndex];

                foreach (var queue in queues)
                {
                    fetchedJob = _connection.JobQueue.FindOneAndUpdate(
                            fetchCondition & Builders<JobQueueDto>.Filter.Eq(_ => _.Queue, queue),
                            Builders<JobQueueDto>.Update.Set(_ => _.FetchedAt, _connection.GetServerTimeUtc()),
                            new FindOneAndUpdateOptions<JobQueueDto>
                            {
                                IsUpsert = false,
                                ReturnDocument = ReturnDocument.After
                            }, cancellationToken);
                    if (fetchedJob != null)
                    {
                        break;
                    }
                }

                if (fetchedJob == null)
                {
                    if (currentQueryIndex == fetchConditions.Length - 1)
                    {
                        cancellationToken.WaitHandle.WaitOne(_options.QueuePollInterval);
                        cancellationToken.ThrowIfCancellationRequested();
                    }
                }

                currentQueryIndex = (currentQueryIndex + 1) % fetchConditions.Length;
            }
            while (fetchedJob == null);

            return new MongoFetchedJob(_connection, fetchedJob.Id, fetchedJob.JobId.ToString(CultureInfo.InvariantCulture), fetchedJob.Queue);
        }

        public void Enqueue(string queue, string jobId)
        {
            _connection
                .JobQueue
                .InsertOne(new JobQueueDto
                {
                    JobId = int.Parse(jobId),
                    Queue = queue
                });
        }
    }
#pragma warning disable 1591
}