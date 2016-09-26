using System;
using Hangfire.Mongo.Dto;
using Hangfire.Mongo.MongoUtils;
using MongoDB.Bson;
using MongoDB.Driver;

namespace Hangfire.Mongo.Database
{
    /// <summary>
    /// Represents Mongo database context for Hangfire
    /// </summary>
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1063:ImplementIDisposableCorrectly")]
    public class HangfireDbContext : IDisposable
    {
        private const int RequiredSchemaVersion = 5;

        private readonly string _prefix;

        internal IMongoDatabase Database { get; }

        /// <summary>
        /// Constructs context with connection string and database name
        /// </summary>
        /// <param name="connectionString">Connection string for Mongo database</param>
        /// <param name="databaseName">Database name</param>
        /// <param name="prefix">Collections prefix</param>
        public HangfireDbContext(string connectionString, string databaseName, string prefix = "hangfire")
        {
            _prefix = prefix;

            var client = new MongoClient(connectionString);

            Database = client.GetDatabase(databaseName);

            ConnectionId = Guid.NewGuid().ToString();
        }

        /// <summary>
        /// Constructs context with Mongo client settings and database name
        /// </summary>
        /// <param name="mongoClientSettings">Client settings for MongoDB</param>
        /// <param name="databaseName">Database name</param>
        /// <param name="prefix">Collections prefix</param>
        public HangfireDbContext(MongoClientSettings mongoClientSettings, string databaseName, string prefix = "hangfire")
        {
            _prefix = prefix;

            var client = new MongoClient(mongoClientSettings);

            Database = client.GetDatabase(databaseName);

            ConnectionId = Guid.NewGuid().ToString();
        }

        /// <summary>
        /// Constructs context with existing Mongo database connection
        /// </summary>
        /// <param name="database">Database connection</param>
        public HangfireDbContext(IMongoDatabase database)
        {
            Database = database;
            ConnectionId = Guid.NewGuid().ToString();
        }

        /// <summary>
        /// Mongo database connection identifier
        /// </summary>
        public string ConnectionId { get; private set; }

        /// <summary>
        /// Reference to collection which contains identifiers
        /// </summary>
        public virtual IMongoCollection<IdentifierDto> Identifiers => Database.GetCollection<IdentifierDto>(_prefix + "_identifiers");

        /// <summary>
        /// Reference to collection which contains distributed locks
        /// </summary>
        public virtual IMongoCollection<DistributedLockDto> DistributedLock => Database.GetCollection<DistributedLockDto>(_prefix + ".locks");

        /// <summary>
        /// Reference to collection which contains counters
        /// </summary>
        public virtual IMongoCollection<CounterDto> Counter => Database.GetCollection<CounterDto>(_prefix + ".counter");

        /// <summary>
        /// Reference to collection which contains aggregated counters
        /// </summary>
        public virtual IMongoCollection<AggregatedCounterDto> AggregatedCounter => Database.GetCollection<AggregatedCounterDto>(_prefix + ".aggregatedcounter");

        /// <summary>
        /// Reference to collection which contains hashes
        /// </summary>
        public virtual IMongoCollection<HashDto> Hash => Database.GetCollection<HashDto>(_prefix + ".hash");

        /// <summary>
        /// Reference to collection which contains jobs
        /// </summary>
        public virtual IMongoCollection<JobDto> Job => Database.GetCollection<JobDto>(_prefix + ".job");

        /// <summary>
        /// Reference to collection which contains jobs parameters
        /// </summary>
        public virtual IMongoCollection<JobParameterDto> JobParameter => Database.GetCollection<JobParameterDto>(_prefix + ".jobParameter");

        /// <summary>
        /// Reference to collection which contains jobs queues
        /// </summary>
        public virtual IMongoCollection<JobQueueDto> JobQueue => Database.GetCollection<JobQueueDto>(_prefix + ".jobQueue");

        /// <summary>
        /// Reference to collection which contains lists
        /// </summary>
        public virtual IMongoCollection<ListDto> List => Database.GetCollection<ListDto>(_prefix + ".list");

        /// <summary>
        /// Reference to collection which contains schemas
        /// </summary>
        public virtual IMongoCollection<SchemaDto> Schema => Database.GetCollection<SchemaDto>(_prefix + ".schema");

        /// <summary>
        /// Reference to collection which contains servers information
        /// </summary>
        public virtual IMongoCollection<ServerDto> Server => Database.GetCollection<ServerDto>(_prefix + ".server");

        /// <summary>
        /// Reference to collection which contains sets
        /// </summary>
        public virtual IMongoCollection<SetDto> Set => Database.GetCollection<SetDto>(_prefix + ".set");

        /// <summary>
        /// Reference to collection which contains states
        /// </summary>
        public virtual IMongoCollection<StateDto> State => Database.GetCollection<StateDto>(_prefix + ".state");

        /// <summary>
        /// Initializes intial collections schema for Hangfire
        /// </summary>
        public void Init()
        {
            var schema = Schema.Find(new BsonDocument()).FirstOrDefault();
            if (schema != null)
            {
                if (RequiredSchemaVersion > schema.Version)
                {
                    Schema.DeleteMany(new BsonDocument());
                    Schema.InsertOne(new SchemaDto { Version = RequiredSchemaVersion });
                }
                else if (RequiredSchemaVersion < schema.Version)
                {
                    throw new InvalidOperationException($"HangFire current database schema version {schema.Version} is newer than the configured MongoStorage schema version {RequiredSchemaVersion}. Please update to the latest HangFire.Mongo NuGet package.");
                }
            }
            else
            {
                Schema.InsertOne(new SchemaDto { Version = RequiredSchemaVersion });
            }

            CreateJobIndexes();
        }


        private void CreateJobIndexes()
        {
            // Create for jobid on state, jobParameter, jobQueue
            State.CreateDescendingIndex(p => p.JobId);
            JobParameter.CreateDescendingIndex(p => p.JobId);
            JobQueue.CreateDescendingIndex(p => p.JobId);
        }


        /// <summary>
        /// Disposes the object
        /// </summary>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1063:ImplementIDisposableCorrectly",
            Justification = "Dispose should only implement finalizer if owning an unmanaged resource")]
        public void Dispose()
        {
        }
    }
}