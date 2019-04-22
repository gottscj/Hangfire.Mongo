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
    internal class MongoNotificationObserver : IBackgroundProcess, IServerComponent
    {
        private readonly HangfireDbContext _dbContext;
        private readonly IJobQueueSemaphore _jobQueueSemaphore;
        private readonly IDistributedLockMutex _distributedLockMutex;

        public MongoNotificationObserver(HangfireDbContext dbContext, IJobQueueSemaphore jobQueueSemaphore,
            IDistributedLockMutex distributedLockMutex)
        {
            _dbContext = dbContext;
            _jobQueueSemaphore = jobQueueSemaphore;
            _distributedLockMutex = distributedLockMutex;
        }

        public void Execute(CancellationToken cancellationToken)
        {
            var options = new FindOptions<NotificationDto> {CursorType = CursorType.TailableAwait};

            var lastId = ObjectId.GenerateNewId(new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc));
            var filter = new BsonDocument("_id", new BsonDocument("$gt", lastId));

            var update = Builders<NotificationDto>
                .Update
                .SetOnInsert(j => j.Value, null);

            var lastEnqueued = _dbContext.Notifications.FindOneAndUpdate(filter, update,
                new FindOneAndUpdateOptions<NotificationDto>
                {
                    IsUpsert = true,
                    Sort = Builders<NotificationDto>.Sort.Descending(j => j.Id),
                    ReturnDocument = ReturnDocument.After
                });

            lastId = lastEnqueued.Id;
            filter = new BsonDocument("_id", new BsonDocument("$gt", lastId));

            while (!cancellationToken.IsCancellationRequested)
            {
                // Start the cursor and wait for the initial response
                using (var cursor = _dbContext.Notifications.FindSync(filter, options, cancellationToken))
                {
                    foreach (var eventDto in cursor.ToEnumerable(cancellationToken))
                    {
                        // Set the last value we saw 
                        lastId = eventDto.Id;
                        if (string.IsNullOrEmpty(eventDto.Value))
                        {
                            continue;
                        }
                        switch (eventDto.Type)
                        {
                            case NotificationType.JobEnqueued:
                                _jobQueueSemaphore.Release(eventDto.Value);
                                break;
                            case NotificationType.LockReleased:
                                _distributedLockMutex.Release(eventDto.Value);
                                break;
                            
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
    }
}