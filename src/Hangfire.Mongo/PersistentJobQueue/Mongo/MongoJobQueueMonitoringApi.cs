using System;
using System.Collections.Generic;
using System.Linq;
using Hangfire.Mongo.Database;
using Hangfire.Mongo.Dto;
using MongoDB.Bson;
using MongoDB.Driver;

namespace Hangfire.Mongo.PersistentJobQueue.Mongo
{
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
            return _connection.JobQueue
                .Find(new BsonDocument())
                .Project(_ => _.Queue)
                .ToList().Distinct().ToList();
        }

        public IEnumerable<int> GetEnqueuedJobIds(string queue, int @from, int perPage)
        {
            return _connection.JobQueue
                .Find(Builders<JobQueueDto>.Filter.Eq(_ => _.Queue, queue) & Builders<JobQueueDto>.Filter.Eq(_ => _.FetchedAt, null))
                .Skip(@from)
                .Limit(perPage)
                .Project(_ => _.JobId)
                .ToList()
                .Where(jobQueueJobId =>
                {
                    var job = _connection.Job.Find(Builders<JobDto>.Filter.Eq(_ => _.Id, jobQueueJobId)).FirstOrDefault();
                    return (job != null) && (_connection.State.Find(Builders<StateDto>.Filter.Eq(_ => _.Id, job.StateId)).FirstOrDefault() != null);
                }).ToArray();
        }

        public IEnumerable<int> GetFetchedJobIds(string queue, int @from, int perPage)
        {
            return _connection.JobQueue
                .Find(Builders<JobQueueDto>.Filter.Eq(_ => _.Queue, queue) & Builders<JobQueueDto>.Filter.Ne(_ => _.FetchedAt, null))
                .Skip(@from)
                .Limit(perPage)
                .Project(_ => _.JobId)
                .ToList()
                .Where(jobQueueJobId =>
                {
                    var job = _connection.Job.Find(Builders<JobDto>.Filter.Eq(_ => _.Id, jobQueueJobId)).FirstOrDefault();
                    return job != null;
                }).ToArray();
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
		
    }
}