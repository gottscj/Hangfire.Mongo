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
            return AsyncHelper.RunSync(() => _connection.JobQueue.Find(new BsonDocument()).ToListAsync())
                .Select(x => x.Queue)
                .Distinct()
                .ToList();
        }

        public IEnumerable<int> GetEnqueuedJobIds(string queue, int @from, int perPage)
        {
            int start = @from + 1;
            int end = from + perPage;

            return AsyncHelper.RunSync(() => _connection.JobQueue.Find(
                        Builders<JobQueueDto>.Filter.Eq(_ => _.Queue, queue) &
                        Builders<JobQueueDto>.Filter.Eq(_ => _.FetchedAt, null)
                    ).ToListAsync())
                .Select((data, i) => new { Index = i + 1, Data = data })
                .Where(_ => (_.Index >= start) && (_.Index <= end))
                .Select(x => x.Data)
                .Where(jobQueue =>
                {
                    var job = AsyncHelper.RunSync(() => _connection.Job.Find(Builders<JobDto>.Filter.Eq(_ => _.Id, jobQueue.JobId)).FirstOrDefaultAsync());
                    return (job != null) && (AsyncHelper.RunSync(() => _connection.State.Find(Builders<StateDto>.Filter.Eq(_ => _.Id, job.StateId)).FirstOrDefaultAsync()) != null);
                })
                .Select(jobQueue => jobQueue.Id)
                .ToArray();
        }

        public IEnumerable<int> GetFetchedJobIds(string queue, int @from, int perPage)
        {
            int start = @from + 1;
            int end = from + perPage;

            return AsyncHelper.RunSync(() => _connection.JobQueue
                .Find(Builders<JobQueueDto>.Filter.Eq(_ => _.Queue, queue) & Builders<JobQueueDto>.Filter.Ne(_ => _.FetchedAt, null)).ToListAsync())
                .Select((data, i) => new { Index = i + 1, Data = data })
                .Where(_ => (_.Index >= start) && (_.Index <= end))
                .Select(x => x.Data)
                .Where(jobQueue => AsyncHelper.RunSync(() => _connection.Job.Find(Builders<JobDto>.Filter.Eq(_ => _.Id, jobQueue.JobId)).FirstOrDefaultAsync()) != null)
                .Select(jobQueue => jobQueue.Id)
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