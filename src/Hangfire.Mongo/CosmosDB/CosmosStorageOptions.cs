using System;

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
            CheckQueuedJobsStrategy = CheckQueuedJobsStrategy.Watch;
            CheckConnection = false;
            MigrationLockTimeout = TimeSpan.FromMinutes(2);
            Factory = new CosmosFactory();
        }
    }
}