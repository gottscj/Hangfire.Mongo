using System;
using System.Data;
using System.Linq;
using Hangfire.Mongo.Database;
using Hangfire.Mongo.Dto;
using Hangfire.Mongo.MongoUtils;
using Hangfire.Mongo.Tests.Utils;
using Moq;
using Xunit;

namespace Hangfire.Mongo.Tests
{
	public class MongoFetchedJobFacts
	{
		private const string JobId = "id";
		private const string Queue = "queue";


		[Fact]
		public void Ctor_ThrowsAnException_WhenConnectionIsNull()
		{
			UseConnection(connection =>
			{
				var exception = Assert.Throws<ArgumentNullException>(
					() => new MongoFetchedJob(null, 1, JobId, Queue));

				Assert.Equal("connection", exception.ParamName);
			});
		}

		[Fact]
		public void Ctor_ThrowsAnException_WhenJobIdIsNull()
		{
			UseConnection(connection =>
			{
				var exception = Assert.Throws<ArgumentNullException>(() => new MongoFetchedJob(connection, 1, null, Queue));

				Assert.Equal("jobId", exception.ParamName);
			});
		}

		[Fact]
		public void Ctor_ThrowsAnException_WhenQueueIsNull()
		{
			UseConnection(connection =>
			{
				var exception = Assert.Throws<ArgumentNullException>(
					() => new MongoFetchedJob(connection, 1, JobId, null));

				Assert.Equal("queue", exception.ParamName);
			});
		}

		[Fact]
		public void Ctor_CorrectlySets_AllInstanceProperties()
		{
			UseConnection(connection =>
			{
				var fetchedJob = new MongoFetchedJob(connection, 1, JobId, Queue);

				Assert.Equal(1, fetchedJob.Id);
				Assert.Equal(JobId, fetchedJob.JobId);
				Assert.Equal(Queue, fetchedJob.Queue);
			});
		}

		[Fact, CleanDatabase]
		public void RemoveFromQueue_ReallyDeletesTheJobFromTheQueue()
		{
			UseConnection(connection =>
			{
				// Arrange
				var id = CreateJobQueueRecord(connection, "1", "default");
				var processingJob = new MongoFetchedJob(connection, id, "1", "default");

				// Act
				processingJob.RemoveFromQueue();

				// Assert
				var count = connection.JobQueue.Count();
				Assert.Equal(0, count);
			});
		}

		[Fact, CleanDatabase]
		public void RemoveFromQueue_DoesNotDelete_UnrelatedJobs()
		{
			UseConnection(connection =>
			{
				// Arrange
				CreateJobQueueRecord(connection, "1", "default");
				CreateJobQueueRecord(connection, "1", "critical");
				CreateJobQueueRecord(connection, "2", "default");

				var fetchedJob = new MongoFetchedJob(connection, 999, "1", "default");

				// Act
				fetchedJob.RemoveFromQueue();

				// Assert
				var count = connection.JobQueue.Count();
				Assert.Equal(3, count);
			});
		}

		[Fact, CleanDatabase]
		public void Requeue_SetsFetchedAtValueToNull()
		{
			UseConnection(connection =>
			{
				// Arrange
				var id = CreateJobQueueRecord(connection, "1", "default");
				var processingJob = new MongoFetchedJob(connection, id, "1", "default");

				// Act
				processingJob.Requeue();

				// Assert
				var record = connection.JobQueue.FindAll().Single();
				Assert.Null(record.FetchedAt);
			});
		}

		[Fact, CleanDatabase]
		public void Dispose_SetsFetchedAtValueToNull_IfThereWereNoCallsToComplete()
		{
			UseConnection(sql =>
			{
				// Arrange
				var id = CreateJobQueueRecord(sql, "1", "default");
				var processingJob = new MongoFetchedJob(sql, id, "1", "default");

				// Act
				processingJob.Dispose();

				// Assert
				var record = sql.JobQueue.FindAll().Single();
				Assert.Null(record.FetchedAt);
			});
		}

		private static int CreateJobQueueRecord(HangfireDbContext connection, string jobId, string queue)
		{
			var jobQueue = new JobQueueDto
			{
				JobId = int.Parse(jobId),
				Queue = queue,
				FetchedAt = connection.GetServerTimeUtc()
			};

			connection.JobQueue.Insert(jobQueue);

			return jobQueue.Id;
		}

		private static void UseConnection(Action<HangfireDbContext> action)
		{
			using (var connection = ConnectionUtils.CreateConnection())
			{
				action(connection);
			}
		}
	}
}