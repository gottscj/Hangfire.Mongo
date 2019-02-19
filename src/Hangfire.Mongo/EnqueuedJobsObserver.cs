using System;
using System.Threading;
using Hangfire.Mongo.Database;
using Hangfire.Mongo.Dto;
using Hangfire.Server;
using MongoDB.Bson;
using MongoDB.Driver;

namespace Hangfire.Mongo
{
    /// <summary>
    /// Observes if jobs are enqueued and signals 
    /// </summary>
    internal class EnqueuedJobsObserver : IBackgroundProcess, IServerComponent
    {
        private readonly HangfireDbContext _dbContext;
        private readonly IJobQueueSemaphore _jobQueueSemaphore;

        public EnqueuedJobsObserver(HangfireDbContext dbContext, IJobQueueSemaphore jobQueueSemaphore)
        {
            _dbContext = dbContext;
            _jobQueueSemaphore = jobQueueSemaphore;
        }
        
        public void Execute(CancellationToken cancellationToken)
        {
            var options = new FindOptions<JobEnqueuedDto> { CursorType = CursorType.TailableAwait };

            var lastId = ObjectId.GenerateNewId(new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc));
            var filter = new BsonDocument("_id", new BsonDocument("$gt", lastId));

            var update = Builders<JobEnqueuedDto>
                .Update
                .SetOnInsert(j => j.Queue, null);

            var lastEnqueued = _dbContext.EnqueuedJobs.FindOneAndUpdate(filter, update,
                new FindOneAndUpdateOptions<JobEnqueuedDto>
                {
                    IsUpsert = true,
                    Sort = Builders<JobEnqueuedDto>.Sort.Descending(j => j.Id),
                    ReturnDocument = ReturnDocument.After
                });

            lastId = lastEnqueued.Id;
            filter = new BsonDocument("_id", new BsonDocument("$gt", lastId));
            
            while (!cancellationToken.IsCancellationRequested)
            {
                // Start the cursor and wait for the initial response
                using (var cursor = _dbContext.EnqueuedJobs.FindSync(filter, options, cancellationToken))
                {
                    foreach (var jobEnqueuedDto in cursor.ToEnumerable(cancellationToken))
                    {
                        // Set the last value we saw 
                        lastId = jobEnqueuedDto.Id;
                        var queue = jobEnqueuedDto.Queue;
                        
                        if (string.IsNullOrEmpty(queue))
                        {
                            continue;
                        }
                        _jobQueueSemaphore.Release(queue);
                    }
                }

                // The tailable cursor died so loop through and restart it
                // Now, we want documents that are strictly greater than the last value we saw
                filter = new BsonDocument("_id", new BsonDocument("$gt", lastId));

                // cursor died, restart it
            }
        }

        public void Execute(BackgroundProcessContext context)
        {
            Execute(context.CancellationToken);
        }
    }
}