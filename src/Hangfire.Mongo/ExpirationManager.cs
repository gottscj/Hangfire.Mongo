using System;
using System.Linq.Expressions;
using System.Threading;
using Hangfire.Logging;
using Hangfire.Mongo.Database;
using Hangfire.Mongo.Dto;
using Hangfire.Mongo.MongoUtils;
using Hangfire.Server;
using MongoDB.Driver;

namespace Hangfire.Mongo
{
    /// <summary>
    /// Represents Hangfire expiration manager for Mongo database
    /// </summary>
    public class ExpirationManager : IBackgroundProcess, IServerComponent
    {
        private static readonly ILog Logger = LogProvider.For<ExpirationManager>();

        private readonly MongoStorage _storage;
        private readonly TimeSpan _checkInterval;

        /// <summary>
        /// Constructs expiration manager with one hour checking interval
        /// </summary>
        /// <param name="storage">MongoDB storage</param>
        public ExpirationManager(MongoStorage storage)
            : this(storage, TimeSpan.FromHours(1))
        {
        }

        /// <summary>
        /// Constructs expiration manager with specified checking interval
        /// </summary>
        /// <param name="storage">MongoDB storage</param>
        /// <param name="checkInterval">Checking interval</param>
        public ExpirationManager(MongoStorage storage, TimeSpan checkInterval)
        {
            if (storage == null)
                throw new ArgumentNullException(nameof(storage));

            _storage = storage;
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
            using (HangfireDbContext connection = _storage.CreateAndOpenConnection())
            {
                DateTime now = connection.GetServerTimeUtc();

                RemoveExpiredRecord(connection.Job, _ => _.ExpireAt, now);
                RemoveExpiredRecord(connection.StateData.OfType<ExpiringKeyValueDto>(), _ => _.ExpireAt, now);
            }

            cancellationToken.WaitHandle.WaitOne(_checkInterval);
        }

        /// <summary>
        /// Returns text representation of the object
        /// </summary>
        public override string ToString()
        {
            return "Mongo Expiration Manager";
        }

        private static void RemoveExpiredRecord<TEntity, TField>(IMongoCollection<TEntity> collection, Expression<Func<TEntity, TField>> expression, TField now)
        {
            Logger.DebugFormat("Removing outdated records from table '{0}'...", collection.CollectionNamespace.CollectionName);

            collection.DeleteMany(Builders<TEntity>.Filter.Lt(expression, now));
        }
    }
}