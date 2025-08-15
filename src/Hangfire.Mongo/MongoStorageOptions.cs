using System;
using Hangfire.Mongo.UtcDateTime;

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
        private MongoMigrationOptions _migrationOptions;
        private MongoFactory _factory;
        private UtcDateTimeStrategy[] _enabledUtcDateTimeStrategies;
        private string _prefix;

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

            _enabledUtcDateTimeStrategies =
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
            get => _factory;
            set
            {
                if (value == null)
                {
                    throw new ArgumentException($"'{nameof(Factory)}' cannot be null");
                }
                _factory = value;
            }
        }

        /// <summary>
        /// Collection name prefix for all Hangfire related collections
        /// </summary>
        public string Prefix
        {
            get => _prefix;
            set
            {
                if (value == null)
                {
                    throw new ArgumentException($"'{nameof(Prefix)}' cannot be null");
                }
                _prefix = value;
            }
        }

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
        public TimeSpan? SlidingInvisibilityTimeout { get; set; }

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
            get => _migrationOptions;
            set
            {
                if (value == null)
                {
                    throw new ArgumentException($"'{nameof(MigrationOptions)}' cannot be null");
                }
                _migrationOptions = value;
            }
        }

        /// <summary>
        /// Array of enabled strategies for obtaining UTC date and time.
        /// </summary>
        public UtcDateTimeStrategy[] EnabledUtcDateTimeStrategies
        {
            get => _enabledUtcDateTimeStrategies;
            set
            {
                if (value == null)
                {
                    throw new ArgumentException($"'{EnabledUtcDateTimeStrategies}' cannot be null");
                }
                _enabledUtcDateTimeStrategies = value;
            }
        }

        /// <summary>
        /// Custom strategy for resolving UTC time from MongoDB
        /// </summary>
        public UtcDateTimeStrategy UtcDateTimeStrategy { get; set; }
    }
}