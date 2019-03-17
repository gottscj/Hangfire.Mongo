using System;

namespace Hangfire.Mongo
{
    /// <summary>
    /// Represents Hangfire storage options for MongoDB implementation
    /// </summary>
    public class MongoStorageOptions
    {
        private TimeSpan _queuePollInterval;

        private TimeSpan _distributedLockLifetime;

        private TimeSpan _migrationLockTimeout;
        
        /// <summary>
        /// Constructs storage options with default parameters
        /// </summary>
        public MongoStorageOptions()
        {
            Prefix = "hangfire";
            QueuePollInterval = TimeSpan.FromSeconds(15);
            InvisibilityTimeout = TimeSpan.FromMinutes(30);
            DistributedLockLifetime = TimeSpan.FromSeconds(30);
            JobExpirationCheckInterval = TimeSpan.FromHours(1);
            CountersAggregateInterval = TimeSpan.FromMinutes(5);
            MigrationLockTimeout = TimeSpan.FromSeconds(30);
            
            ClientId = Guid.NewGuid().ToString().Replace("-", string.Empty);

            MigrationOptions = new MongoMigrationOptions();
        }

        /// <summary>
        /// Collection name prefix for all Hangfire related collections
        /// </summary>
        public string Prefix { get; set; }

        /// <summary>
        /// Poll interval for queue
        /// </summary>
        public TimeSpan QueuePollInterval
        {
            get { return _queuePollInterval; }
            set
            {
                var message = $"The QueuePollInterval property value should be positive. Given: {value}.";

                if (value == TimeSpan.Zero)
                {
                    throw new ArgumentException(message, nameof(value));
                }
                if (value != value.Duration())
                {
                    throw new ArgumentException(message, nameof(value));
                }

                _queuePollInterval = value;
            }
        }

        /// <summary>
        /// Invisibility timeout
        /// </summary>
        [Obsolete("This is marked obsolete in Hangfire and is not used anymore")]
        public TimeSpan InvisibilityTimeout { get; set; }

        /// <summary>
        /// Lifetime of distributed lock
        /// </summary>
        public TimeSpan DistributedLockLifetime
        {
            get { return _distributedLockLifetime; }
            set
            {
                var message = $"The DistributedLockLifetime property value should be positive. Given: {value}.";

                if (value == TimeSpan.Zero)
                {
                    throw new ArgumentException(message, nameof(value));
                }
                if (value != value.Duration())
                {
                    throw new ArgumentException(message, nameof(value));
                }

                _distributedLockLifetime = value;
            }
        }

        /// <summary>
        /// Timeout for other process to wait before timing out when waiting for migration to complete
        /// default = 30 seconds 
        /// </summary>
        /// <exception cref="ArgumentException"></exception>
        public TimeSpan MigrationLockTimeout
        {
            get { return _migrationLockTimeout; }
            set
            {
                var message = $"The MigrationLockTimeout property value should be positive. Given: {value}.";

                if (value == TimeSpan.Zero)
                {
                    throw new ArgumentException(message, nameof(value));
                }
                if (value != value.Duration())
                {
                    throw new ArgumentException(message, nameof(value));
                }

                _migrationLockTimeout = value;
            }
        }

        /// <summary>
        /// Client identifier
        /// </summary>
        public string ClientId { get; }

        /// <summary>
        /// Expiration check inteval for jobs
        /// </summary>
        public TimeSpan JobExpirationCheckInterval { get; set; }

        /// <summary>
        /// Counters interval
        /// </summary>
        public TimeSpan CountersAggregateInterval { get; set; }


        /// <summary>
        /// The options used if migration is needed
        /// </summary>
        public MongoMigrationOptions MigrationOptions { get; set; }
    }
}