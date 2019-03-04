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
    internal class MongoEventListener : IBackgroundProcess, IServerComponent
    {
        private readonly HangfireDbContext _dbContext;
        private readonly IJobQueueSemaphore _jobQueueSemaphore;

        public MongoEventListener(HangfireDbContext dbContext, IJobQueueSemaphore jobQueueSemaphore)
        {
            _dbContext = dbContext;
            _jobQueueSemaphore = jobQueueSemaphore;
        }
        
        public void Execute(CancellationToken cancellationToken)
        {
            var options = new FindOptions<EventDto> { CursorType = CursorType.TailableAwait };

            var lastId = ObjectId.GenerateNewId(new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc));
            var filter = new BsonDocument("_id", new BsonDocument("$gt", lastId));

            var update = Builders<EventDto>
                .Update
                .SetOnInsert(j => j.Value, null);

            var lastEnqueued = _dbContext.Events.FindOneAndUpdate(filter, update,
                new FindOneAndUpdateOptions<EventDto>
                {
                    IsUpsert = true,
                    Sort = Builders<EventDto>.Sort.Descending(j => j.Id),
                    ReturnDocument = ReturnDocument.After
                });

            lastId = lastEnqueued.Id;
            filter = new BsonDocument("_id", new BsonDocument("$gt", lastId));
            
            while (!cancellationToken.IsCancellationRequested)
            {
                // Start the cursor and wait for the initial response
                using (var cursor = _dbContext.Events.FindSync(filter, options, cancellationToken))
                {
                    foreach (var eventDto in cursor.ToEnumerable(cancellationToken))
                    {
                        // Set the last value we saw 
                        lastId = eventDto.Id;
                        switch (eventDto.Type)
                        {
                            case EventType.JobEnqueued:
                            {
                                ReleaseQueue(eventDto.Value);
                                break;
                            }
                        }
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

        private void ReleaseQueue(string queue)
        {
            if (string.IsNullOrEmpty(queue))
            {
                return;
            }
            _jobQueueSemaphore.Release(queue);
        }
    }
}