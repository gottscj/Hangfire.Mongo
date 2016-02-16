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


            ClientId = Guid.NewGuid().ToString().Replace("-", String.Empty);
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
                var message = String.Format("The QueuePollInterval property value should be positive. Given: {0}.",
	                value);

                if (value == TimeSpan.Zero)
                {
                    throw new ArgumentException(message, "value");
                }
                if (value != value.Duration())
                {
                    throw new ArgumentException(message, "value");
                }

                _queuePollInterval = value;
            }
        }

        /// <summary>
        /// Invisibility timeout
        /// </summary>
        public TimeSpan InvisibilityTimeout { get; set; }

        /// <summary>
        /// Lifetime of distributed lock
        /// </summary>
        public TimeSpan DistributedLockLifetime
        {
            get { return _distributedLockLifetime; }
            set
            {
                var message = String.Format(
	                "The DistributedLockLifetime property value should be positive. Given: {0}.", value);

                if (value == TimeSpan.Zero)
                {
                    throw new ArgumentException(message, "value");
                }
                if (value != value.Duration())
                {
                    throw new ArgumentException(message, "value");
                }

                _distributedLockLifetime = value;
            }
        }

        /// <summary>
        /// Cleint identifier
        /// </summary>
        public string ClientId { get; private set; }

	    /// <summary>
        /// Expiration check inteval for jobs
        /// </summary>
        public TimeSpan JobExpirationCheckInterval { get; set; }

        /// <summary>
        /// Counters interval
        /// </summary>
        public TimeSpan CountersAggregateInterval { get; set; }

    }
}