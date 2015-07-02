using Hangfire.Mongo.Database;
using Hangfire.Mongo.Dto;
using MongoDB.Driver.Builders;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Hangfire.Mongo.PersistentJobQueue.Mongo
{
#pragma warning disable 1591
    internal class MongoJobQueueMonitoringApi : IPersistentJobQueueMonitoringApi
    {
        private readonly HangfireDbContext _connection;

        public MongoJobQueueMonitoringApi(HangfireDbContext connection)
        {
            if (connection == null)
                throw new ArgumentNullException("connection");

            _connection = connection;
        }

        public IEnumerable<string> GetQueues()
        {
            return _connection.JobQueue.FindAll().Select(x => x.Queue).Distinct().ToList();
        }

        public IEnumerable<int> GetEnqueuedJobIds(string queue, int @from, int perPage)
        {
            int start = @from + 1;
            int end = from + perPage;

            return _connection.JobQueue
                .Find(Query.And(Query<JobQueueDto>.EQ(_ => _.Queue, queue), Query<JobQueueDto>.EQ(_ => _.FetchedAt, null)))
                .Select((data, i) => new { Index = i + 1, Data = data })
                .Where(_ => (_.Index >= start) && (_.Index <= end))
                .Select(x => x.Data)
                .Where(jobQueue =>
                {
                    var job = _connection.Job.FindOneById(jobQueue.JobId);
                    return (job != null) && (_connection.State.FindOneById(job.StateId) != null);
                })
                .Select(jobQueue => jobQueue.Id)
                .ToArray();
        }

        public IEnumerable<int> GetFetchedJobIds(string queue, int @from, int perPage)
        {
            int start = @from + 1;
            int end = from + perPage;

            return _connection.JobQueue
                .Find(Query.And(Query<JobQueueDto>.EQ(_ => _.Queue, queue), Query<JobQueueDto>.NE(_ => _.FetchedAt, null)))
                .Select((data, i) => new { Index = i + 1, Data = data })
                .Where(_ => (_.Index >= start) && (_.Index <= end))
                .Select(x => x.Data)
                .Where(jobQueue => _connection.Job.FindOneById(jobQueue.JobId) != null)
                .Select(jobQueue => jobQueue.Id)
                .ToArray();
        }

        public EnqueuedAndFetchedCountDto GetEnqueuedAndFetchedCount(string queue)
        {
            var enqueuedCount = (int)_connection.JobQueue
                .Count(Query.And(Query<JobQueueDto>.EQ(_ => _.Queue, queue),
                    Query<JobQueueDto>.EQ(_ => _.FetchedAt, null)));

            var fetchedCount = (int)_connection.JobQueue
                .Count(Query.And(Query<JobQueueDto>.EQ(_ => _.Queue, queue),
                    Query<JobQueueDto>.NE(_ => _.FetchedAt, null)));

            return new EnqueuedAndFetchedCountDto
            {
                EnqueuedCount = enqueuedCount,
                FetchedCount = fetchedCount
            };
        }

        // ReSharper disable once ClassNeverInstantiated.Local
        private class JobIdDto
        {
            public int Id { get; set; }
        }
    }
#pragma warning restore 1591
}