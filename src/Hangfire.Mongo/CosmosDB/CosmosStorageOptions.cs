using System;
using Hangfire.Mongo.Migration.Strategies;
using Hangfire.Mongo.Migration.Strategies.Backup;

namespace Hangfire.Mongo.CosmosDB
{
    /// <summary>
    /// Cosmos storage options
    /// </summary>
    public class CosmosStorageOptions : MongoStorageOptions
    {
        /// <summary>
        /// ctor
        /// </summary>
        public CosmosStorageOptions()
        {
            UseNotificationsCollection = false;
            CheckConnection = false;
            UseTransactions = false;
            MigrationLockTimeout = TimeSpan.FromMinutes(2);
        }
    }
}