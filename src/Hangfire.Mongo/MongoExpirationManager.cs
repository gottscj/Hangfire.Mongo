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
    /// Represents Hangfire expiration manager for Mongo database
    /// </summary>
    public class MongoExpirationManager : IBackgroundProcess, IBackgroundProcessAsync, IServerComponent
    {
        private static readonly ILog Logger = LogProvider.For<MongoExpirationManager>();

        private readonly HangfireDbContext _dbContext;
        private readonly TimeSpan _checkInterval;

        /// <summary>
        /// Constructs expiration manager with one hour checking interval
        /// </summary>
        /// <param name="dbContext">MongoDB storage</param>
        /// <param name="options"></param>
        public MongoExpirationManager(HangfireDbContext dbContext, MongoStorageOptions options)
        {
            _dbContext = dbContext;
            _checkInterval = options.JobExpirationCheckInterval;
        }

        /// <summary>
        /// Run expiration manager to remove outdated records
        /// </summary>
        /// <param name="context">Background processing context</param>
        public void Execute(BackgroundProcessContext context)
        {
            Execute(context.StoppingToken);
        }

        /// <summary>
        /// Run expiration manager to remove outdated records asynchronously.
        /// </summary>
        /// <param name="context">Background processing context</param>
        public Task ExecuteAsync(BackgroundProcessContext context)
        {
            return ExecuteAsync(context.StoppingToken);
        }

        /// <summary>
        /// Run expiration manager to remove outdated records
        /// </summary>
        /// <param name="cancellationToken">Cancellation token</param>
        public void Execute(CancellationToken cancellationToken)
        {
            var filter = CreateExpirationFilter();

            var result = _dbContext.JobGraph.DeleteMany(filter);

            LogDeletedCount(result.DeletedCount);

            cancellationToken.WaitHandle.WaitOne(_checkInterval);
        }

        /// <summary>
        /// Run expiration manager to remove outdated records asynchronously.
        /// </summary>
        /// <param name="cancellationToken">Cancellation token</param>
        public async Task ExecuteAsync(CancellationToken cancellationToken)
        {
            var filter = CreateExpirationFilter();

            // Match the previous sync behavior: once a cleanup pass starts, finish the
            // delete operation. Cancellation is only used to skip the following wait.
            var result = await _dbContext.JobGraph.DeleteManyAsync(filter, CancellationToken.None)
                .ConfigureAwait(false);

            LogDeletedCount(result.DeletedCount);

            await Delay(_checkInterval, cancellationToken).ConfigureAwait(false);
        }

        private static BsonDocument CreateExpirationFilter()
        {
            return new BsonDocument
            {
                ["_t"] = nameof(ExpiringJobDto),
                [nameof(ExpiringJobDto.ExpireAt)] = new BsonDocument
                {
                    ["$lt"] = DateTime.UtcNow
                }
            };
        }

        private void LogDeletedCount(long deletedCount)
        {
            if (Logger.IsDebugEnabled())
            {
                Logger.DebugFormat($"Removed {deletedCount} outdated " +
                                   $"documents from '{_dbContext.JobGraph.CollectionNamespace.CollectionName}'.");
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
}
