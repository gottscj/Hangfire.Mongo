using Hangfire.Annotations;
using Hangfire.Mongo.Database;
using Hangfire.Mongo.Dto;
using Hangfire.Mongo.MongoUtils;
using Hangfire.Storage;
using MongoDB.Driver;
using MongoDB.Driver.Builders;
using System;
using System.Globalization;
using System.Threading;

namespace Hangfire.Mongo.PersistentJobQueue.Mongo
{
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

			JobQueueDto fetchedJob;

			var fetchConditions = new[]
			{
				Query<JobQueueDto>.EQ(_ => _.FetchedAt, null),
				Query<JobQueueDto>.LT(_ => _.FetchedAt, _connection.GetServerTimeUtc().AddSeconds(_options.InvisibilityTimeout.Negate().TotalSeconds))
			};
			var currentQueryIndex = 0;

			do
			{
				cancellationToken.ThrowIfCancellationRequested();

				fetchedJob = _connection.JobQueue
					.FindAndModify(new FindAndModifyArgs
					{
						Query = Query.And(fetchConditions[currentQueryIndex], Query<JobQueueDto>.In(_ => _.Queue, queues)),
						Update = Update<JobQueueDto>.Set(_ => _.FetchedAt, _connection.GetServerTimeUtc()),
						VersionReturned = FindAndModifyDocumentVersion.Modified,
						Upsert = false
					})
					.GetModifiedDocumentAs<JobQueueDto>();

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
			_connection.JobQueue.Insert(new JobQueueDto
			{
				JobId = int.Parse(jobId),
				Queue = queue
			});
		}
	}
}