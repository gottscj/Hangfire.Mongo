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
    /// uses mongo feature watch to observe locks and added jobs
    /// </summary>
    public class MongoJobQueueWatcher : IBackgroundProcess, IServerComponent
    {
        private static readonly ILog Logger = LogProvider.For<MongoJobQueueWatcher>();
        private readonly HangfireDbContext _dbContext;
        private readonly MongoStorageOptions _storageOptions;
        private readonly IJobQueueSemaphore _jobQueueSemaphore;

        /// <summary>
        /// ctor
        /// </summary>
        /// <param name="dbContext"></param>
        /// <param name="storageOptions"></param>
        /// <param name="jobQueueSemaphore"></param>
        public MongoJobQueueWatcher(
            HangfireDbContext dbContext,
            MongoStorageOptions storageOptions,
            IJobQueueSemaphore jobQueueSemaphore)
        {
            _dbContext = dbContext;
            _storageOptions = storageOptions;
            _jobQueueSemaphore = jobQueueSemaphore;

        }
        /// <inheritdoc />
        public void Execute(CancellationToken cancellationToken)
        {
            var pipeline = new EmptyPipelineDefinition<ChangeStreamDocument<BsonDocument>>()
                .Match(j =>
                    j.OperationType == ChangeStreamOperationType.Insert && j.FullDocument["_t"] == nameof(JobQueueDto));
            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    var cursor = _dbContext.Database.GetCollection<BsonDocument>(_dbContext.JobGraph.CollectionNamespace.CollectionName).Watch(pipeline);
                    if (Logger.IsTraceEnabled())
                    {
                        Logger.Trace("Watcher: Watching for enqueued jobs");
                    }
                    
                    foreach (var change in cursor.ToEnumerable(cancellationToken))
                    {
                        var queue = change.FullDocument[nameof(JobQueueDto.Queue)].AsString;
                        _jobQueueSemaphore.Release(queue);
                        if (Logger.IsTraceEnabled())
                        {
                            Logger.Trace("Watcher: Job enqueued, queue: " + queue);
                        }
                    }
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
                    cancellationToken.WaitHandle.WaitOne(MongoNotificationObserver.MaxTimeout);
                }
                
            }
        }

        /// <inheritdoc />
        public void Execute(BackgroundProcessContext context)
        {
            Execute(context.StoppingToken);
        }
    }
}