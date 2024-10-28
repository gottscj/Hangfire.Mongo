using System;
using System.Threading;
using Hangfire.Logging;
using Hangfire.Mongo.Database;
using Hangfire.Mongo.Dto;
using Hangfire.Mongo.Migration;
using Hangfire.Server;
using MongoDB.Bson;

namespace Hangfire.Mongo
{
    /// <summary>
    /// Represents Hangfire expiration manager for Mongo database
    /// </summary>
    public class MongoExpirationManager : IBackgroundProcess, IServerComponent
    {
        private readonly MongoStorageOptions _options;
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
            _options = options;
            _checkInterval = options.JobExpirationCheckInterval;
        }

        /// <summary>
        /// Run expiration manager to remove outdated records
        /// </summary>
        /// <param name="context">Background processing context</param>
        public void Execute(BackgroundProcessContext context)
        {
            Execute(context.CancellationToken);
        }

        /// <summary>
        /// Run expiration manager to remove outdated records
        /// </summary>
        /// <param name="cancellationToken">Cancellation token</param>
        public void Execute(CancellationToken cancellationToken)
        {
            var result = _dbContext
                .JobGraph
                .DeleteMany(new BsonDocument
                {
                    ["_t"] = nameof(ExpiringJobDto),
                    [nameof(ExpiringJobDto.ExpireAt)] = new BsonDocument
                    {
                        ["$lt"] = DateTime.UtcNow
                    }
                });

            if(Logger.IsDebugEnabled())
            {
                Logger.DebugFormat($"Removed {result.DeletedCount} outdated " +
                               $"documents from '{_dbContext.JobGraph.CollectionNamespace.CollectionName}'.");                
            }
            
            cancellationToken.WaitHandle.WaitOne(_checkInterval);
            // if we are closing down, try to delete the migration lock for good measures.
            if (cancellationToken.IsCancellationRequested)
            {
                new MigrationLock(_dbContext.Database, _options.Prefix, _options.MigrationLockTimeout)
                    .DeleteMigrationLock();
            }
        }
    }
}