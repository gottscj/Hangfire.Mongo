using System;
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
    /// uses mongo feature watch to observe locks and added jobs
    /// </summary>
    public class AsyncMongoJobQueueWatcher : MongoJobQueueWatcher, IBackgroundProcessAsync
    {

        /// <summary>
        /// ctor
        /// </summary>
        /// <param name="dbContext"></param>
        /// <param name="storageOptions"></param>
        /// <param name="jobQueueSemaphore"></param>
        public AsyncMongoJobQueueWatcher(
            HangfireDbContext dbContext,
            MongoStorageOptions storageOptions,
            IJobQueueSemaphore jobQueueSemaphore) : base(dbContext, storageOptions, jobQueueSemaphore)
        {

        }
        
        /// <summary>
        /// executes async
        /// </summary>
        /// <param name="context"></param>
        public async Task ExecuteAsync(BackgroundProcessContext context)
        {
            await ExecuteAsync(context.StoppingToken);
        }
        
        /// <summary>
        /// executes async
        /// </summary>
        /// <param name="cancellationToken"></param>
        public async Task ExecuteAsync(CancellationToken cancellationToken)
        {
            var pipeline = CreatePipeline();

            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    using var cursor = await DbContext
                        .Database
                        .GetCollection<BsonDocument>(DbContext.JobGraph.CollectionNamespace.CollectionName)
                        .WatchAsync<BsonDocument>(pipeline, cancellationToken: cancellationToken)
                        .ConfigureAwait(false);

                    if (Logger.IsTraceEnabled())
                    {
                        Logger.Trace("Watcher: Watching for enqueued jobs");
                    }

                    while (await cursor.MoveNextAsync(cancellationToken).ConfigureAwait(false))
                    {
                        foreach (var change in cursor.Current)
                        {
                            ProcessChange(change);
                        }
                    }
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                }
                catch (MongoCommandException e)
                {
                    if (e.Message.Contains("$changeStream stage is only supported on replica sets"))
                    {
                        Logger.ErrorException(
                            "Current db does not support change stream (not a replica set, https://docs.mongodb.com/manual/reference/method/db.collection.watch/)\r\n" +
                                    "if you need instant (almost) handling of enqueued jobs, please set 'CheckQueuedJobsStrategy' to 'TailNotificationsCollection' in MongoStorageOptions", e);
                        throw;
                    }
                    // wait max allowed
                    await Delay(MongoNotificationObserver.MaxTimeout, cancellationToken).ConfigureAwait(false);
                }

            }
        }
        

        private static BsonDocument[] CreatePipeline()
        {
            return
            [
                new BsonDocument
                {
                    ["$match"] = new BsonDocument
                    {
                        ["operationType"] =  "update",
                        [$"updateDescription.updatedFields.{nameof(JobDto.Queue)}"] = new BsonDocument
                        {
                            ["$exists"] = true
                        },
                        [$"updateDescription.updatedFields.{nameof(JobDto.Queue)}"] = new BsonDocument
                        {
                            ["$ne"] = BsonNull.Value
                        }
                    }
                }
            ];
        }

        private void ProcessChange(BsonDocument change)
        {
            var queue = change["updateDescription"]["updatedFields"][nameof(JobDto.Queue)].AsString;
            JobQueueSemaphore.Release(queue);
            if (Logger.IsTraceEnabled())
            {
                Logger.Trace("Watcher: Job enqueued, queue: " + queue);
            }
        }

        /// <summary>
        /// Waits for retry interval while treating cooperative shutdown as a normal exit path.
        /// </summary>
        /// <param name="millisecondsDelay">Delay duration in milliseconds.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        protected static async Task Delay(int millisecondsDelay, CancellationToken cancellationToken)
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
