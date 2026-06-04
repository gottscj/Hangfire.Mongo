using System;
using System.Threading;
using System.Threading.Tasks;
using Hangfire.Logging;
using Hangfire.Mongo.Database;
using Hangfire.Mongo.Dto;
using Hangfire.Server;
using Hangfire.States;
using MongoDB.Bson;
using MongoDB.Driver;

namespace Hangfire.Mongo.CosmosDB;

/// <summary>
/// async handler
/// </summary>
public class AsyncCosmosQueueWatcher : MongoJobQueueWatcher, IBackgroundProcessAsync
{

    /// <summary>
    /// ctor
    /// </summary>
    /// <param name="dbContext"></param>
    /// <param name="storageOptions"></param>
    /// <param name="jobQueueSemaphore"></param>
    public AsyncCosmosQueueWatcher(HangfireDbContext dbContext,
        MongoStorageOptions storageOptions,
        IJobQueueSemaphore jobQueueSemaphore)
        : base(dbContext, storageOptions, jobQueueSemaphore)
    {
    }

    /// <summary>
    /// executes async
    /// </summary>
    /// <param name="context"></param>
    /// <returns></returns>
    public Task ExecuteAsync(BackgroundProcessContext context) => ExecuteAsync(context.StoppingToken);

    /// <summary>
    /// executes async
    /// </summary>
    /// <param name="cancellationToken"></param>
    protected virtual async Task ExecuteAsync(CancellationToken cancellationToken)
    {
        Logger.Warn("Be careful using Watch. Its not thoroughly tested!");
        var pipeline = CreatePipeline();

        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                using var cursor = await DbContext
                    .Database
                    .GetCollection<BsonDocument>(DbContext.JobGraph.CollectionNamespace.CollectionName)
                    .WatchAsync<BsonDocument>(pipeline, new ChangeStreamOptions
                    {
                        FullDocument = ChangeStreamFullDocumentOption.UpdateLookup
                    }, cancellationToken)
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
                        "if you need instant (almost) handling of enqueued jobs, please set 'CheckQueuedJobsStrategy' to 'TailNotificationsCollection' in MongoStorageOptions",
                        e);
                    throw;
                }

                // wait max allowed
                await Delay(TimeSpan.FromMilliseconds(MongoNotificationObserver.MaxTimeout), cancellationToken)
                    .ConfigureAwait(false);
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
                    ["operationType"] = new BsonDocument
                    {
                        ["$in"] = new BsonArray
                        {
                            "insert", "update", "replace"
                        }
                    }
                }
            },
            new BsonDocument
            {
                ["$project"] = new BsonDocument
                {
                    ["_id"] = 1,
                    ["fullDocument"] = 1,
                    ["ns"] = 1,
                    ["documentKey"] = 1
                }
            }
        ];
    }

    private void ProcessChange(BsonDocument change)
    {
        var doc = change["fullDocument"].AsBsonDocument;
        var types = doc["_t"].AsBsonArray;

        if (!types.Contains("JobDto"))
        {
            return;
        }

        var stateName = doc[nameof(JobDto.StateName)];
        if (stateName == BsonNull.Value ||
            stateName != EnqueuedState.StateName)
        {
            return;
        }

        // var queue = change["updateDescription"]["updatedFields"][nameof(JobDto.Queue)].AsString;
        JobQueueSemaphore.Release(doc[nameof(JobDto.Queue)].AsString);
        if (Logger.IsTraceEnabled())
        {
            Logger.Trace("Watcher: Job enqueued, queue: " + doc[nameof(JobDto.Queue)].AsString);
        }
    }
    private static async Task Delay(TimeSpan delay, CancellationToken cancellationToken)
    {
        try
        {
            await Task.Delay(delay, cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
        }
    }
}
