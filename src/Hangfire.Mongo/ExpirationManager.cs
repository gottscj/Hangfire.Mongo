using Hangfire.Logging;
using Hangfire.Mongo.Database;
using Hangfire.Mongo.MongoUtils;
using Hangfire.Server;
using System;
using System.Threading;
using MongoDB.Driver;
using MongoDB.Driver.Builders;

namespace Hangfire.Mongo
{
    /// <summary>
    /// Represents Hangfire expiration manager for Mongo database
    /// </summary>
    public class ExpirationManager : IServerComponent
    {
        private static readonly ILog Logger = LogProvider.GetCurrentClassLogger();

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
                throw new ArgumentNullException("storage");

            _storage = storage;
            _checkInterval = checkInterval;
        }

        /// <summary>
        /// Run expiration manager to remove outdated records
        /// </summary>
        /// <param name="cancellationToken">Cancellation token</param>
        public void Execute(CancellationToken cancellationToken)
        {
            using (HangfireDbContext connection = _storage.CreateAndOpenConnection())
            {
                MongoCollection[] processedTables =
				{
					connection.Counter,
					connection.Job,
					connection.List,
					connection.Set,
					connection.Hash
				};

                DateTime now = connection.GetServerTimeUtc();
                foreach (var table in processedTables)
                {
                    Logger.DebugFormat("Removing outdated records from table '{0}'...", table.Name);

                    table.Remove(Query.LT("ExpireAt", now));
                }
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
    }
}