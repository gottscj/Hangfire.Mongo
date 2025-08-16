using System;
using Hangfire.Mongo.UtcDateTime;

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
            CheckQueuedJobsStrategy = CheckQueuedJobsStrategy.Poll;
            CheckConnection = false;
            MigrationLockTimeout = TimeSpan.FromMinutes(2);
            Factory = new CosmosFactory();
            SupportsCappedCollection = false;
            UtcDateTimeStrategies =
            [
                new IsMasterUtcDateTimeStrategy()
            ];
        }
    }
}