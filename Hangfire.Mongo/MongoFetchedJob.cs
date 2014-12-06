using System;
using Hangfire.Mongo.Database;
using Hangfire.Mongo.Dto;
using Hangfire.Storage;
using MongoDB.Driver;
using MongoDB.Driver.Builders;

namespace Hangfire.Mongo
{
	public class MongoFetchedJob : IFetchedJob
	{
		private readonly HangfireDbContext _connection;

		private bool _disposed;

		private bool _removedFromQueue;

		private bool _requeued;

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


		public int Id { get; private set; }

		public string JobId { get; private set; }

		public string Queue { get; private set; }

		public void Dispose()
		{
			if (_disposed) return;

			if (!_removedFromQueue && !_requeued)
			{
				Requeue();
			}

			_disposed = true;
		}

		public void RemoveFromQueue()
		{
			_connection.JobQueue.Remove(Query<JobQueueDto>.EQ(_ => _.Id, Id));

			_removedFromQueue = true;
		}

		public void Requeue()
		{
			_connection.JobQueue.Update(Query<JobQueueDto>.EQ(_ => _.Id, Id),
				Update<JobQueueDto>.Set(_ => _.FetchedAt, null), UpdateFlags.None);

			_requeued = true;
		}
	}
}