using System;
using Hangfire.Mongo.Dto;
using MongoDB.Driver;

namespace Hangfire.Mongo.Database
{
    /// <summary>
    /// Represents Mongo database context for Hangfire
    /// </summary>
    public sealed class HangfireDbContext
    {
        private readonly string _prefix;

        internal MongoClient Client { get; }

        internal IMongoDatabase Database { get; }

        private HangfireDbContext(string prefix)
        {
            _prefix = prefix;
            ConnectionId = Guid.NewGuid().ToString();
        }

        /// <summary>
        /// Constructs context with connection string and database name
        /// </summary>
        /// <param name="connectionString">Connection string for Mongo database</param>
        /// <param name="databaseName">Database name</param>
        /// <param name="prefix">Collections prefix</param>
        public HangfireDbContext(string connectionString, string databaseName, string prefix = "hangfire")
            : this(prefix)
        {
            Client = new MongoClient(connectionString);
            Database = Client.GetDatabase(databaseName);
        }

        /// <summary>
        /// Constructs context with Mongo client settings and database name
        /// </summary>
        /// <param name="mongoClientSettings">Client settings for MongoDB</param>
        /// <param name="databaseName">Database name</param>
        /// <param name="prefix">Collections prefix</param>
        public HangfireDbContext(MongoClientSettings mongoClientSettings, string databaseName, string prefix = "hangfire")
            : this(prefix)
        {
            var client = new MongoClient(mongoClientSettings);
            Database = client.GetDatabase(databaseName);
        }

        /// <summary>
        /// Constructs context with existing Mongo database connection
        /// </summary>
        /// <param name="database">Database connection</param>
        /// <param name="prefix">Collections prefix</param>
        public HangfireDbContext(IMongoDatabase database, string prefix = "hangfire")
            : this(prefix)
        {
            Database = database;
        }

        /// <summary>
        /// Mongo database connection identifier
        /// </summary>
        public string ConnectionId { get; private set; }

        /// <summary>
        /// Reference to collection which contains various state information
        /// </summary>
        public IMongoCollection<KeyValueDto> StateData =>
            Database.GetCollection<KeyValueDto>(_prefix + ".stateData");

        /// <summary>
        /// Reference to collection which contains distributed locks
        /// </summary>
        public IMongoCollection<DistributedLockDto> DistributedLock => Database
            .GetCollection<DistributedLockDto>(_prefix + ".locks");

        /// <summary>
        /// Reference to collection which contains jobs
        /// </summary>
        public IMongoCollection<JobDto> Job => Database.GetCollection<JobDto>(_prefix + ".job");

        /// <summary>
        /// Reference to collection which contains jobs queues
        /// </summary>
        public IMongoCollection<JobQueueDto> JobQueue => Database.GetCollection<JobQueueDto>(_prefix + ".jobQueue");

        /// <summary>
        /// Reference to collection which is used for signalling
        /// </summary>
        public IMongoCollection<SignalDto> Signal => Database.GetCollection<SignalDto>(_prefix + ".signal");

        /// <summary>
        /// Reference to collection which contains schemas
        /// </summary>
        public IMongoCollection<SchemaDto> Schema => Database.GetCollection<SchemaDto>(_prefix + ".schema");

        /// <summary>
        /// Reference to collection which contains servers information
        /// </summary>
        public IMongoCollection<ServerDto> Server => Database.GetCollection<ServerDto>(_prefix + ".server");

    }
}