using System;
using System.Threading;
using Hangfire.Logging;
using Hangfire.Mongo.Database;
using Hangfire.Mongo.Dto;
using Hangfire.Server;
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
        public MongoExpirationManager(HangfireDbContext dbContext)
            : this(dbContext, TimeSpan.FromHours(1))
        {
        }

        /// <summary>
        /// Constructs expiration manager with specified checking interval
        /// </summary>
        /// <param name="dbContext">MongoDB storage</param>
        /// <param name="checkInterval">Checking interval</param>
        public MongoExpirationManager(HangfireDbContext dbContext, TimeSpan checkInterval)
        {
            _dbContext = dbContext;
            _checkInterval = checkInterval;
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
            var outDatedFilter = Builders<ExpiringJobDto>
                .Filter
                .Lt(_ => _.ExpireAt, DateTime.UtcNow);
            
            var result = _dbContext
                .JobGraph
                .OfType<ExpiringJobDto>()
                .DeleteMany(outDatedFilter);

            if(Logger.IsDebugEnabled())
            {
                Logger.DebugFormat($"Removed {result.DeletedCount} outdated " +
                               $"documents from '{_dbContext.JobGraph.CollectionNamespace.CollectionName}'.");                
            }
            
            cancellationToken.WaitHandle.WaitOne(_checkInterval);
        }
    }
}