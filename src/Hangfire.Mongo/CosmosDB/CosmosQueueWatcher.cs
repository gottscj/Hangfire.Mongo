using System.Threading;
using Hangfire.Logging;
using Hangfire.Mongo.Database;
using Hangfire.Mongo.Dto;
using Hangfire.States;
using MongoDB.Bson;
using MongoDB.Driver;

namespace Hangfire.Mongo.CosmosDB;

/// <summary>
/// 
/// </summary>
public class CosmosQueueWatcher : MongoJobQueueWatcher
{

    /// <summary>
    /// ctor
    /// </summary>
    /// <param name="dbContext"></param>
    /// <param name="storageOptions"></param>
    /// <param name="jobQueueSemaphore"></param>
    public CosmosQueueWatcher(HangfireDbContext dbContext,
        MongoStorageOptions storageOptions,
        IJobQueueSemaphore jobQueueSemaphore) 
        : base(dbContext, storageOptions, jobQueueSemaphore)
    {
    }

    /// <inheritdoc />
    public override void Execute(CancellationToken cancellationToken)
    {
        Logger.Warn("Be careful using Watch. Its not thoroughly tested!");
        var pipeline = new[]
        {
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
        };

        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                var cursor = DbContext
                    .Database
                    .GetCollection<BsonDocument>(DbContext.JobGraph.CollectionNamespace.CollectionName)
                    .Watch<BsonDocument>(pipeline, new ChangeStreamOptions
                    {
                        FullDocument = ChangeStreamFullDocumentOption.UpdateLookup
                    });

                if (Logger.IsTraceEnabled())
                {
                    Logger.Trace("Watcher: Watching for enqueued jobs");
                }

                foreach (var change in cursor.ToEnumerable(cancellationToken))
                {
                    var doc = change["fullDocument"].AsBsonDocument;
                    var types = doc["_t"].AsBsonArray;

                    if (!types.Contains("JobDto"))
                    {
                        continue;
                    }

                    var stateName = doc[nameof(JobDto.StateName)];
                    if (stateName == BsonNull.Value || 
                        stateName != EnqueuedState.StateName)
                    {
                        continue;
                    }
                    
                    // var queue = change["updateDescription"]["updatedFields"][nameof(JobDto.Queue)].AsString;
                    JobQueueSemaphore.Release(doc[nameof(JobDto.Queue)].AsString);
                    if (Logger.IsTraceEnabled())
                    {
                        Logger.Trace("Watcher: Job enqueued, queue: " + doc[nameof(JobDto.Queue)].AsString);
                    }
                }
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
                cancellationToken.WaitHandle.WaitOne(MongoNotificationObserver.MaxTimeout);
            }
        }
    }
}