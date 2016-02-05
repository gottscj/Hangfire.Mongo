using Hangfire.Mongo.Database;
using Hangfire.Mongo.Dto;
using System;
using System.Collections.Generic;
using System.Linq;
using MongoDB.Bson;
using MongoDB.Driver;

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
            return _connection.JobQueue.Find(new BsonDocument()).ToList().Select(x => x.Queue).Distinct().ToList();
        }

        public IEnumerable<int> GetEnqueuedJobIds(string queue, int @from, int perPage)
        {
            int start = @from + 1;
            int end = from + perPage;

            return _connection.JobQueue.Find(
                        Builders<JobQueueDto>.Filter.Eq(_ => _.Queue, queue) &
                        Builders<JobQueueDto>.Filter.Eq(_ => _.FetchedAt, null)
                    ).ToList()
                .Select((data, i) => new { Index = i + 1, Data = data })
                .Where(_ => (_.Index >= start) && (_.Index <= end))
                .Select(x => x.Data)
                .Where(jobQueue =>
                {
                    var job = _connection.Job.Find(Builders<JobDto>.Filter.Eq(_ => _.Id, jobQueue.JobId)).FirstOrDefault();
                    return (job != null) && (_connection.State.Find(Builders<StateDto>.Filter.Eq(_ => _.Id, job.StateId)).FirstOrDefault() != null);
                })
                .Select(jobQueue => jobQueue.JobId)
                .ToArray();
        }

        public IEnumerable<int> GetFetchedJobIds(string queue, int @from, int perPage)
        {
            int start = @from + 1;
            int end = from + perPage;

            return _connection.JobQueue
                .Find(Builders<JobQueueDto>.Filter.Eq(_ => _.Queue, queue) & Builders<JobQueueDto>.Filter.Ne(_ => _.FetchedAt, null)).ToList()
                .Select((data, i) => new { Index = i + 1, Data = data })
                .Where(_ => (_.Index >= start) && (_.Index <= end))
                .Select(x => x.Data)
                .Where(jobQueue => _connection.Job.Find(Builders<JobDto>.Filter.Eq(_ => _.Id, jobQueue.JobId)).FirstOrDefault() != null)
                .Select(jobQueue => jobQueue.Id)
                .ToArray();
        }

        public EnqueuedAndFetchedCountDto GetEnqueuedAndFetchedCount(string queue)
        {
            int enqueuedCount = (int)_connection.JobQueue.Count(Builders<JobQueueDto>.Filter.Eq(_ => _.Queue, queue) &
                                                                Builders<JobQueueDto>.Filter.Eq(_ => _.FetchedAt, null));

            int fetchedCount = (int)_connection.JobQueue.Count(Builders<JobQueueDto>.Filter.Eq(_ => _.Queue, queue) &
                                                               Builders<JobQueueDto>.Filter.Ne(_ => _.FetchedAt, null));

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