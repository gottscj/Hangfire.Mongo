using System;

namespace Hangfire.Mongo
{
    /// <summary>
    /// Represents Hangfire storage options for MongoDB implementation
    /// </summary>
    public class AsyncMongoStorageOptions : MongoStorageOptions
    {
        /// <summary>
        /// Constructs storage options with default parameters
        /// </summary>
        public AsyncMongoStorageOptions()
        {
            Prefix = "hangfire";
            QueuePollInterval = TimeSpan.FromSeconds(15);
            SlidingInvisibilityTimeout = TimeSpan.FromMinutes(5);
            DistributedLockLifetime = TimeSpan.FromSeconds(30);
            JobExpirationCheckInterval = TimeSpan.FromHours(1);
            CountersAggregateInterval = TimeSpan.FromMinutes(5);
            MigrationLockTimeout = TimeSpan.FromSeconds(30);
            CheckConnection = true;
            ByPassMigration = false;
            ConnectionCheckTimeout = TimeSpan.FromSeconds(5);
            
            MigrationOptions = new MongoMigrationOptions();
            Factory = new AsyncMongoFactory();
            CheckQueuedJobsStrategy = CheckQueuedJobsStrategy.Watch;
        }
    }
}
