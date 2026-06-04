using System;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
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
    public class AsyncMongoNotificationObserver : MongoNotificationObserver, IBackgroundProcessAsync
    {
        private static readonly ILog Logger = LogProvider.For<MongoNotificationObserver>();
        private readonly HangfireDbContext _dbContext;
        private readonly IJobQueueSemaphore _jobQueueSemaphore;

        /// <summary>
        /// ctor
        /// </summary>
        /// <param name="dbContext"></param>
        /// <param name="storageOptions"></param>
        /// <param name="jobQueueSemaphore"></param>
        public AsyncMongoNotificationObserver(
            HangfireDbContext dbContext,
            MongoStorageOptions storageOptions,
            IJobQueueSemaphore jobQueueSemaphore) : base(dbContext, storageOptions, jobQueueSemaphore)
        {
            _dbContext = dbContext;
            _jobQueueSemaphore = jobQueueSemaphore;
        }


        /// <summary>
        /// Invoked by Hangfire.Core
        /// </summary>
        /// <param name="cancellationToken"></param>
        public virtual async Task ExecuteAsync(CancellationToken cancellationToken)
        {
            var options = new FindOptions<BsonDocument> { CursorType = CursorType.TailableAwait };

            var lastId = ObjectId.GenerateNewId(new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc));
            var filter = new BsonDocument("_id", new BsonDocument("$gt", lastId));

            var update = new BsonDocument
            {
                ["$setOnInsert"] = new BsonDocument(nameof(NotificationDto.Value), BsonNull.Value)
            };
            var lastEnqueued = await _dbContext.Notifications.FindOneAndUpdateAsync(filter, update,
                    new FindOneAndUpdateOptions<BsonDocument>
                    {
                        IsUpsert = true,
                        Sort = new BsonDocument("_id", -1),
                        ReturnDocument = ReturnDocument.After
                    }, cancellationToken)
                .ConfigureAwait(false);

            lastId = lastEnqueued["_id"].AsObjectId;
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
                    using (var cursor = await _dbContext.Notifications.FindAsync(filter, options, cancellationToken)
                               .ConfigureAwait(false))
                    {
                        while (await cursor.MoveNextAsync(cancellationToken).ConfigureAwait(false))
                        {
                            foreach (var doc in cursor.Current)
                            {
                                lastId = ProcessNotification(doc);
                            }
                        }
                    }
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                }
                catch (MongoCommandException commandException)
                {
                    if (HasOverriddenSyncCommandExceptionHandler())
                    {
                        HandleMongoCommandException(commandException, cancellationToken);
                    }
                    else
                    {
                        await HandleMongoCommandExceptionAsync(commandException, cancellationToken)
                            .ConfigureAwait(false);
                    }
                }
                catch (Exception e)
                {
                    Logger.Error(
                        $"Error observing '{_dbContext.Notifications.CollectionNamespace.CollectionName}'\r\n{e}");
                    await Delay(GetFailureTimeoutMs(), cancellationToken).ConfigureAwait(false);
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
        public virtual Task ExecuteAsync(BackgroundProcessContext context)
        {
            return ExecuteAsync(context.StoppingToken);
        }

        private bool HasOverriddenSyncCommandExceptionHandler()
        {
            var method = GetType().GetMethod(
                nameof(HandleMongoCommandException),
                BindingFlags.Instance | BindingFlags.NonPublic,
                binder: null,
                types: new[] { typeof(MongoCommandException), typeof(CancellationToken) },
                modifiers: null);

            return method != null && method.DeclaringType != typeof(MongoNotificationObserver);
        }

        /// <summary>
        /// Default:
        ///     If error contains "tailable cursor requested on non capped collection."
        ///    then try to convert
        /// </summary>
        protected virtual async Task HandleMongoCommandExceptionAsync(MongoCommandException commandException,
            CancellationToken cancellationToken)
        {
            var successfullyRecreatedCollection = false;
            if (commandException.Message.Contains("tailable cursor requested on non capped collection."))
            {
                Logger.Warn(
                    $"'{_dbContext.Notifications.CollectionNamespace.CollectionName}' collection is not capped.\r\n" +
                    "Trying to convert to capped");
                try
                {
                    await _dbContext.Database.RunCommandAsync<BsonDocument>(new BsonDocument
                    {
                        ["convertToCapped"] = _dbContext.Notifications.CollectionNamespace.CollectionName,
                        ["size"] = 1048576 * 16, // 16 MB,
                        ["max"] = 100000
                    }, cancellationToken: cancellationToken)
                        .ConfigureAwait(false);
                    // _storageOptions.CreateNotificationsCollection(_dbContext.Database);
                    successfullyRecreatedCollection = true;
                }
                catch (Exception e)
                {
                    Logger.Warn(
                        $"Failed to convert '{_dbContext.Notifications.CollectionNamespace.CollectionName}' with message: {e.Message}");
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
                await Delay(GetFailureTimeoutMs(), cancellationToken).ConfigureAwait(false);
            }
        }

        private ObjectId ProcessNotification(BsonDocument doc)
        {
            // Set the last value we saw
            var notification = new NotificationDto(doc);
            var lastId = notification.Id;
            if (string.IsNullOrEmpty(notification.Value))
            {
                return lastId;
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

            return lastId;
        }

        private static async Task Delay(int millisecondsDelay, CancellationToken cancellationToken)
        {
            try
            {
                await Task.Delay(millisecondsDelay, cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
            }
        }
    }
}
