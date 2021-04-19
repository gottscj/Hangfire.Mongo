using System;
using System.Linq;
using System.Threading;
using Hangfire.Logging;
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
    public class MongoNotificationObserver : IBackgroundProcess, IServerComponent
    {
        private static ILog Logger = LogProvider.For<MongoNotificationObserver>();
        private readonly HangfireDbContext _dbContext;
        private readonly IJobQueueSemaphore _jobQueueSemaphore;
        private readonly IDistributedLockMutex _distributedLockMutex;

        /// <summary>
        /// ctor
        /// </summary>
        /// <param name="dbContext"></param>
        /// <param name="jobQueueSemaphore"></param>
        /// <param name="distributedLockMutex"></param>
        public MongoNotificationObserver(
            HangfireDbContext dbContext, 
            IJobQueueSemaphore jobQueueSemaphore,
            IDistributedLockMutex distributedLockMutex)
        {
            _dbContext = dbContext;
            _jobQueueSemaphore = jobQueueSemaphore;
            _distributedLockMutex = distributedLockMutex;
        }

        /// <summary>
        /// Invoked by Hangfire.Core
        /// </summary>
        /// <param name="cancellationToken"></param>
        public virtual void Execute(CancellationToken cancellationToken)
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
            if (Logger.IsTraceEnabled())
            {
                Logger.Trace("LastId: " + lastId);
            }
            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    // Start the cursor and wait for the initial response
                    using (var cursor = _dbContext.Notifications.FindSync(filter, options, cancellationToken))
                    {
                        foreach (var notification in cursor.ToEnumerable(cancellationToken))
                        {
                            // Set the last value we saw 
                            lastId = notification.Id;
                            if (string.IsNullOrEmpty(notification.Value))
                            {
                                continue;
                            }

                            if (Logger.IsTraceEnabled())
                            {
                                Logger.Trace($"Notification '{notification.Type}': {notification.Value}");
                            }

                            switch (notification.Type)
                            {
                                case NotificationType.JobEnqueued:
                                    _jobQueueSemaphore.Release(notification.Value);
                                    break;
                                case NotificationType.LockReleased:
                                    _distributedLockMutex.Release(notification.Value);
                                    break;
                            }
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                }
                catch (MongoCommandException commandException)
                {
                    var collections = _dbContext
                        .Database.ListCollections().ToList();
                    var notificationsCollection = collections.FirstOrDefault(b =>
                        b["name"].Equals(_dbContext.Notifications.CollectionNamespace.CollectionName));
                    var errorMessage =
                        $"Error observing '{_dbContext.Notifications.CollectionNamespace.CollectionName}'\r\n" +
                        commandException.ErrorMessage;

                    var isCapped = true;
                    if (notificationsCollection != null)
                    {
                        isCapped = notificationsCollection["options"].AsBsonDocument.Contains("capped") &&
                                   notificationsCollection["options"].AsBsonDocument["capped"].AsBoolean;
                    }
                    
                    if (notificationsCollection == null || !isCapped)
                    {
                        errorMessage += "\r\n" + 
                                        "Notifications will not be available\r\n" +
                                        $"If you dropped the '{_dbContext.Notifications.CollectionNamespace.CollectionName}' collection " +
                                        "you need to manually create it again as a capped collection\r\n" +
                                        "For reference, please see\r\n" +
                                        "   - https://docs.mongodb.com/manual/core/capped-collections/\r\n" +
                                        "   - https://github.com/sergeyzwezdin/Hangfire.Mongo/blob/master/src/Hangfire.Mongo/Migration/Steps/Version17/00_AddNotificationsCollection.cs";
                    }
                    
                    
                    Logger.Error(errorMessage);
                    if (notificationsCollection == null || !isCapped)
                    {
                        // fatal error observing notifications. Stop observer.
                        cancellationToken.WaitHandle.WaitOne();
                    }
                }
                catch (Exception e)
                {
                    Logger.Error($"Error observing '{_dbContext.Notifications.CollectionNamespace.CollectionName}'\r\n{e}");
                }
                finally
                {
                    // The tailable cursor died so loop through and restart it
                    // Now, we want documents that are strictly greater than the last value we saw
                    filter = new BsonDocument("_id", new BsonDocument("$gt", lastId));
                }
            }
        }

        /// <summary>
        /// Invoked by Hangfire.Core
        /// </summary>
        /// <param name="context"></param>
        public virtual void Execute(BackgroundProcessContext context)
        {
            Execute(context.CancellationToken);
        }
    }
}