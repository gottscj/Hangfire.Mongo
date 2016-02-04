using Hangfire.Mongo.Database;
using Hangfire.Mongo.Dto;
using Hangfire.Mongo.Helpers;
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
            return AsyncHelper.RunSync(() => _connection.JobQueue
                .Find(new BsonDocument())
                .Project(_ => _.Queue)
                .ToListAsync()).Distinct().ToList();
        }

        public IEnumerable<int> GetEnqueuedJobIds(string queue, int @from, int perPage)
        {
            return AsyncHelper.RunSync(() => _connection.JobQueue
                .Find(Builders<JobQueueDto>.Filter.Eq(_ => _.Queue, queue) & Builders<JobQueueDto>.Filter.Eq(_ => _.FetchedAt, null))
                .Skip(@from)
                .Limit(perPage)
                .Project(_ => _.JobId)
                .ToListAsync())
                .Where(jobQueueJobId =>
                {
                    var job = AsyncHelper.RunSync(() => _connection.Job.Find(Builders<JobDto>.Filter.Eq(_ => _.Id, jobQueueJobId)).FirstOrDefaultAsync());
                    return (job != null) && (AsyncHelper.RunSync(() => _connection.State.Find(Builders<StateDto>.Filter.Eq(_ => _.Id, job.StateId)).FirstOrDefaultAsync()) != null);
                })
                .ToArray();
        }

        public IEnumerable<int> GetFetchedJobIds(string queue, int @from, int perPage)
        {
            return AsyncHelper.RunSync(() => _connection.JobQueue
                .Find(Builders<JobQueueDto>.Filter.Eq(_ => _.Queue, queue) & Builders<JobQueueDto>.Filter.Ne(_ => _.FetchedAt, null))
                .Skip(@from)
                .Limit(perPage)
                .Project(_ => _.JobId)
                .ToListAsync())
                .Where(jobQueueJobId =>
                {
                    var job = AsyncHelper.RunSync(() => _connection.Job.Find(Builders<JobDto>.Filter.Eq(_ => _.Id, jobQueueJobId)).FirstOrDefaultAsync());
                    return job != null;
                })
                .ToArray();
        }

        public EnqueuedAndFetchedCountDto GetEnqueuedAndFetchedCount(string queue)
        {
            int enqueuedCount = (int)AsyncHelper.RunSync(() =>
                _connection.JobQueue.CountAsync(Builders<JobQueueDto>.Filter.Eq(_ => _.Queue, queue) &
                                                Builders<JobQueueDto>.Filter.Eq(_ => _.FetchedAt, null)));

            int fetchedCount = (int)AsyncHelper.RunSync(() =>
                _connection.JobQueue.CountAsync(Builders<JobQueueDto>.Filter.Eq(_ => _.Queue, queue) &
                                                Builders<JobQueueDto>.Filter.Ne(_ => _.FetchedAt, null)));

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