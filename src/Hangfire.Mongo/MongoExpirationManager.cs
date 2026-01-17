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
    /// Represents Hangfire expiration manager for Mongo database
    /// </summary>
    public class MongoExpirationManager : IBackgroundProcess, IServerComponent
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
            Execute(context.CancellationToken);
        }

        /// <summary>
        /// Run expiration manager to remove outdated records
        /// </summary>
        /// <param name="cancellationToken">Cancellation token</param>
        public void Execute(CancellationToken cancellationToken)
        {
            var filter = new BsonDocument
            {
                ["_t"] = nameof(ExpiringJobDto),
                [nameof(ExpiringJobDto.ExpireAt)] = new BsonDocument
                {
                    ["$lt"] = DateTime.UtcNow
                }
            };
            var expiredJobIds = _dbContext.JobGraph
                .Find(filter)
                .Project(j => j["_id"])
                .ToList();
            var deleteFilter = new BsonDocument
            {
                [nameof(JobStateHistoryDto.JobId)] = new BsonDocument("$in", new BsonArray(expiredJobIds))
            };
            
            if (expiredJobIds.Any())
            {
                _dbContext.StateHistory.DeleteMany(deleteFilter);
            }
            
            var result = _dbContext.JobGraph.DeleteMany(filter);

            if (Logger.IsDebugEnabled())
            {
                Logger.DebugFormat($"Removed {result.DeletedCount} outdated " +
                                   $"documents from '{_dbContext.JobGraph.CollectionNamespace.CollectionName}'.");
            }

            cancellationToken.WaitHandle.WaitOne(_checkInterval);
        }
    }
}