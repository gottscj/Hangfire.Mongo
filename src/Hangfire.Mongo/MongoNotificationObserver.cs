using System;
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
        private static readonly ILog Logger = LogProvider.For<MongoNotificationObserver>();
        private readonly HangfireDbContext _dbContext;
        private readonly MongoStorageOptions _storageOptions;
        private readonly IJobQueueSemaphore _jobQueueSemaphore;
        private int _failureTimeout = 5000;
        internal const int MaxTimeout = 60000;

        /// <summary>
        /// ctor
        /// </summary>
        /// <param name="dbContext"></param>
        /// <param name="storageOptions"></param>
        /// <param name="jobQueueSemaphore"></param>
        public MongoNotificationObserver(
            HangfireDbContext dbContext,
            MongoStorageOptions storageOptions,
            IJobQueueSemaphore jobQueueSemaphore)
        {
            _dbContext = dbContext;
            _storageOptions = storageOptions;
            _jobQueueSemaphore = jobQueueSemaphore;
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
                            }
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                }
                catch (MongoCommandException commandException)
                {
                    HandleMongoCommandException(commandException, cancellationToken);
                }
                catch (Exception e)
                {
                    Logger.Error(
                        $"Error observing '{_dbContext.Notifications.CollectionNamespace.CollectionName}'\r\n{e}");
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
            Execute(context.StoppingToken);
        }

        /// <summary>
        /// Default:
        ///     If error contains "tailable cursor requested on non capped collection."
        ///    then try to convert
        /// </summary>
        protected virtual void HandleMongoCommandException(MongoCommandException commandException,
            CancellationToken cancellationToken)
        {
            var successfullyRecreatedCollection = false;
            if (commandException.Message.Contains("tailable cursor requested on non capped collection."))
            {
                Logger.Warn(
                    $"'{_dbContext.Notifications.CollectionNamespace.CollectionName}' collection is not capped.\r\n" +
                    "Trying to drop and creating again");
                try
                {
                    _dbContext.Database.RunCommand<BsonDocument>(new BsonDocument
                    {
                        ["convertToCapped"] = _dbContext.Notifications.CollectionNamespace.CollectionName,
                        ["size"] = 1048576 * 16, // 16 MB,
                        ["max"] = 100000
                    });
                    // _storageOptions.CreateNotificationsCollection(_dbContext.Database);
                    successfullyRecreatedCollection = true;
                }
                catch (Exception e)
                {
                    Logger.Warn(
                        $"Failed to drop and recreate '{_dbContext.Notifications.CollectionNamespace.CollectionName}' with message: {e.Message}");
                }
            }
            else
            {
                var errorMessage =
                    $"Error observing '{_dbContext.Notifications.CollectionNamespace.CollectionName}'\r\n" +
                    commandException.ErrorMessage + "\r\n" +
                    "Notifications will not be available\r\n" +
                    $"If you dropped the '{_dbContext.Notifications.CollectionNamespace.CollectionName}' collection " +
                    "you need to manually create it again as a capped collection\r\n" +
                    "For reference, please see\r\n" +
                    "   - https://docs.mongodb.com/manual/core/capped-collections/\r\n" +
                    "   - https://github.com/sergeyzwezdin/Hangfire.Mongo/blob/master/src/Hangfire.Mongo/Migration/Steps/Version17/00_AddNotificationsCollection.cs";

                Logger.Error(errorMessage);
            }

            if (!successfullyRecreatedCollection)
            {
                // fatal error observing notifications. try again backing off 5s.
                cancellationToken.WaitHandle.WaitOne(GetFailureTimeoutMs());
            }
        }

        /// <summary>
        /// Gets timeout. adds 5 seconds for each call, maximizing at 60s
        /// </summary>
        /// <returns></returns>
        protected virtual int GetFailureTimeoutMs()
        {
            var timeout = _failureTimeout;
            _failureTimeout += 5000;
            if (_failureTimeout >= MaxTimeout)
            {
                _failureTimeout = MaxTimeout;
            }

            return timeout;
        }
    }
}