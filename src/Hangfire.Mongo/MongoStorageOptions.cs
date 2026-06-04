using System;
using Hangfire.Mongo.UtcDateTime;

namespace Hangfire.Mongo
{
    /// <summary>
    /// Represents Hangfire storage options for MongoDB implementation
    /// </summary>
    public class MongoStorageOptions
    {
        private UtcDateTimeStrategy[] _utcDateTimeStrategies;

        /// <summary>
        /// Constructs storage options with default parameters
        /// </summary>
        public MongoStorageOptions()
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

            ClientId = Guid.NewGuid().ToString("N");

            MigrationOptions = new MongoMigrationOptions();
            Factory = new MongoFactory();
            CheckQueuedJobsStrategy = CheckQueuedJobsStrategy.Watch;

            _utcDateTimeStrategies =
            [
                new AggregationUtcDateTimeStrategy(),
                new ServerStatusUtcDateTimeStrategy(),
            ];
        }

        /// <summary>
        /// Strategy for checking for enqueued jobs
        /// </summary>
        public CheckQueuedJobsStrategy CheckQueuedJobsStrategy { get; set; }

        /// <summary>
        /// Factory instance
        /// </summary>
        public MongoFactory Factory
        {
            get;
            set
            {
                if (value == null)
                {
                    throw new ArgumentException($"'{nameof(Factory)}' cannot be null");
                }

                field = value;
            }
        }

        /// <summary>
        /// Collection name prefix for all Hangfire related collections
        /// </summary>
        public string Prefix
        {
            get;
            set
            {
                if (value == null)
                {
                    throw new ArgumentException($"'{nameof(Prefix)}' cannot be null");
                }

                field = value;
            }
        }

        /// <summary>
        /// Poll interval for queue
        /// </summary>
        public TimeSpan QueuePollInterval
        {
            get { return field; }
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

                field = value;
            }
        }

        /// <summary>
        /// Indicates if the underlying storage supports capped collections
        /// default set to true.
        /// MongoStorage will throw NotSupportedException if 'SupportsCappedCollection' is false
        /// and 'CheckQueuedJobsStrategy' is TailNotificationsCollection
        /// </summary>
        public bool SupportsCappedCollection { get; set; } = true;

        /// <summary>
        /// If 'SlidingInvisibilityTimeout' a has value, Hangfire.Mongo will periodically update a jobs timestamp.
        /// 'SlidingInvisibilityTimeout' determines how long time before Hangfire.Mongo decides the job is abandoned
        /// default = 5 min, if set to null, jobs will never be abandoned
        /// </summary>
        public TimeSpan? SlidingInvisibilityTimeout
        {
            get { return field; }
            set
            {
                if (value.HasValue)
                {
                    var message = $"The SlidingInvisibilityTimeout property value should be positive. Given: {value}.";

                    if (value.Value == TimeSpan.Zero)
                    {
                        throw new ArgumentException(message, nameof(value));
                    }
                    if (value.Value != value.Value.Duration())
                    {
                        throw new ArgumentException(message, nameof(value));
                    }
                }

                field = value;
            }
        }

        /// <summary>
        /// Lifetime of distributed lock
        /// </summary>
        public TimeSpan DistributedLockLifetime
        {
            get { return field; }
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

                field = value;
            }
        }

        /// <summary>
        /// Timeout for other process to wait before timing out when waiting for migration to complete
        /// default = 30 seconds 
        /// </summary>
        /// <exception cref="ArgumentException"></exception>
        public TimeSpan MigrationLockTimeout
        {
            get { return field; }
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

                field = value;
            }
        }

        /// <summary>
        /// Client identifier
        /// </summary>
        public string ClientId { get; }

        /// <summary>
        /// Ping database on startup to check connection, if false Hangfire.Mongo will not ping
        /// the db and try to connect to the db using the given MongoClientSettings
        /// </summary>
        public bool CheckConnection { get; set; }

        /// <summary>
        /// Bypass migrations, use at your own risk :)
        /// </summary>
        public bool ByPassMigration { get; set; }

        /// <summary>
        /// Time before cancelling ping to mongo server, if 'CheckConnection' is false, this value will be ignored
        /// </summary>
        public TimeSpan ConnectionCheckTimeout { get; set; }

        /// <summary>
        /// Expiration check interval for jobs
        /// </summary>
        public TimeSpan JobExpirationCheckInterval { get; set; }

        /// <summary>
        /// Counters interval
        /// </summary>
        public TimeSpan CountersAggregateInterval { get; set; }


        /// <summary>
        /// The options used if migration is needed
        /// </summary>
        public MongoMigrationOptions MigrationOptions
        {
            get;
            set
            {
                if (value == null)
                {
                    throw new ArgumentException($"'{nameof(MigrationOptions)}' cannot be null");
                }

                field = value;
            }
        }

        /// <summary>
        /// Array of enabled strategies for obtaining UTC date and time.
        /// </summary>
        public UtcDateTimeStrategy[] UtcDateTimeStrategies
        {
            get => _utcDateTimeStrategies;
            set
            {
                if (value == null)
                {
                    throw new ArgumentException($"'{UtcDateTimeStrategies}' cannot be null");
                }
                _utcDateTimeStrategies = value;
            }
        }
    }
}
