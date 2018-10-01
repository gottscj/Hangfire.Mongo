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
            _connection = connection ?? throw new ArgumentNullException(nameof(connection));
        }

        public IEnumerable<string> GetQueues()
        {
            return _connection.JobGraph.OfType<JobQueueDto>()
                .Find(new BsonDocument())
                .Project(_ => _.Queue)
                .ToList().Distinct().ToList();
        }

        public IEnumerable<string> GetEnqueuedJobIds(string queue, int from, int perPage)
        {
            return _connection.JobGraph.OfType<JobQueueDto>()
                .Find(Builders<JobQueueDto>.Filter.Eq(_ => _.Queue, queue) & Builders<JobQueueDto>.Filter.Eq(_ => _.FetchedAt, null))
                .Skip(from)
                .Limit(perPage)
                .Project(_ => _.JobId)
                .ToList()
                .Where(jobQueueJobId =>
                {
                    return _connection.JobGraph.OfType<JobDto>().Find(j => j.Id == jobQueueJobId && j.StateHistory.Length > 0).Any();
                })
                .Select(jobQueueJobId => jobQueueJobId.ToString())
                .ToArray();
        }

        public IEnumerable<string> GetFetchedJobIds(string queue, int from, int perPage)
        {
            return _connection.JobGraph.OfType<JobQueueDto>()
                .Find(Builders<JobQueueDto>.Filter.Eq(_ => _.Queue, queue) & Builders<JobQueueDto>.Filter.Ne(_ => _.FetchedAt, null))
                .Skip(from)
                .Limit(perPage)
                .Project(_ => _.JobId)
                .ToList()
                .Where(jobQueueJobId =>
                {
                    var job = _connection.JobGraph.OfType<JobDto>().Find(Builders<JobDto>.Filter.Eq(_ => _.Id, jobQueueJobId)).FirstOrDefault();
                    return job != null;
                })
                .Select(jobQueueJobId => jobQueueJobId.ToString())
                .ToArray();
        }

        public EnqueuedAndFetchedCountDto GetEnqueuedAndFetchedCount(string queue)
        {
            int enqueuedCount = (int)_connection.JobGraph.OfType<JobQueueDto>().Count(Builders<JobQueueDto>.Filter.Eq(_ => _.Queue, queue) &
                                                Builders<JobQueueDto>.Filter.Eq(_ => _.FetchedAt, null));

            int fetchedCount = (int)_connection.JobGraph.OfType<JobQueueDto>().Count(Builders<JobQueueDto>.Filter.Eq(_ => _.Queue, queue) &
                                                Builders<JobQueueDto>.Filter.Ne(_ => _.FetchedAt, null));

            return new EnqueuedAndFetchedCountDto
            {
                EnqueuedCount = enqueuedCount,
                FetchedCount = fetchedCount
            };
        }

    }
}