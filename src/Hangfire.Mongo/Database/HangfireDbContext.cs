using Hangfire.Mongo.Dto;
using MongoDB.Driver;
using System;
using System.Linq;
using Hangfire.Mongo.Helpers;
using MongoDB.Bson;

namespace Hangfire.Mongo.Database
{
    /// <summary>
    /// Represents Mongo database context for Hangfire
    /// </summary>
    public class HangfireDbContext : IDisposable
    {
        private const int RequiredSchemaVersion = 4;

        private readonly string _prefix;

        internal IMongoDatabase Database { get; private set; }

        /// <summary>
        /// Constructs context with connection string and database name
        /// </summary>
        /// <param name="connectionString">Connection string for Mongo database</param>
        /// <param name="databaseName">Database name</param>
        /// <param name="prefix">Collections prefix</param>
        public HangfireDbContext(string connectionString, string databaseName, string prefix = "hangfire")
        {
            _prefix = prefix;

            MongoClient client = new MongoClient(connectionString);

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
        public virtual IMongoCollection<IdentifierDto> Identifiers
        {
            get
            {
                return Database.GetCollection<IdentifierDto>(_prefix + "_identifiers");
            }
        }

        /// <summary>
        /// Reference to collection which contains distributed locks
        /// </summary>
        public virtual IMongoCollection<DistributedLockDto> DistributedLock
        {
            get
            {
                return Database.GetCollection<DistributedLockDto>(_prefix + ".locks");
            }
        }

        /// <summary>
        /// Reference to collection which contains counters
        /// </summary>
        public virtual IMongoCollection<CounterDto> Counter
        {
            get
            {
                return Database.GetCollection<CounterDto>(_prefix + ".counter");
            }
        }

        /// <summary>
        /// Reference to collection which contains aggregated counters
        /// </summary>
        public virtual IMongoCollection<AggregatedCounterDto> AggregatedCounter
        {
            get
            {
                return Database.GetCollection<AggregatedCounterDto>(_prefix + ".aggregatedcounter");
            }
        }

        /// <summary>
        /// Reference to collection which contains hashes
        /// </summary>
        public virtual IMongoCollection<HashDto> Hash
        {
            get
            {
                return Database.GetCollection<HashDto>(_prefix + ".hash");
            }
        }

        /// <summary>
        /// Reference to collection which contains jobs
        /// </summary>
        public virtual IMongoCollection<JobDto> Job
        {
            get
            {
                return Database.GetCollection<JobDto>(_prefix + ".job");
            }
        }

        /// <summary>
        /// Reference to collection which contains jobs parameters
        /// </summary>
        public virtual IMongoCollection<JobParameterDto> JobParameter
        {
            get
            {
                return Database.GetCollection<JobParameterDto>(_prefix + ".jobParameter");
            }
        }

        /// <summary>
        /// Reference to collection which contains jobs queues
        /// </summary>
        public virtual IMongoCollection<JobQueueDto> JobQueue
        {
            get
            {
                return Database.GetCollection<JobQueueDto>(_prefix + ".jobQueue");
            }
        }

        /// <summary>
        /// Reference to collection which contains lists
        /// </summary>
        public virtual IMongoCollection<ListDto> List
        {
            get
            {
                return Database.GetCollection<ListDto>(_prefix + ".list");
            }
        }

        /// <summary>
        /// Reference to collection which contains schemas
        /// </summary>
        public virtual IMongoCollection<SchemaDto> Schema
        {
            get
            {
                return Database.GetCollection<SchemaDto>(_prefix + ".schema");
            }
        }

        /// <summary>
        /// Reference to collection which contains servers information
        /// </summary>
        public virtual IMongoCollection<ServerDto> Server
        {
            get
            {
                return Database.GetCollection<ServerDto>(_prefix + ".server");
            }
        }

        /// <summary>
        /// Reference to collection which contains sets
        /// </summary>
        public virtual IMongoCollection<SetDto> Set
        {
            get
            {
                return Database.GetCollection<SetDto>(_prefix + ".set");
            }
        }

        /// <summary>
        /// Reference to collection which contains states
        /// </summary>
        public virtual IMongoCollection<StateDto> State
        {
            get
            {
                return Database.GetCollection<StateDto>(_prefix + ".state");
            }
        }

        /// <summary>
        /// Initializes intial collections schema for Hangfire
        /// </summary>
        public void Init()
        {
            SchemaDto schema = AsyncHelper.RunSync(() => Schema.Find(new BsonDocument()).FirstOrDefaultAsync());

            if (schema != null)
            {
                if (RequiredSchemaVersion > schema.Version)
                {
                    AsyncHelper.RunSync(() => Schema.DeleteManyAsync(new BsonDocument()));
                    AsyncHelper.RunSync(() => Schema.InsertOneAsync(new SchemaDto { Version = RequiredSchemaVersion }));
                }
                else if (RequiredSchemaVersion < schema.Version)
                    throw new InvalidOperationException(String.Format("HangFire current database schema version {0} is newer than the configured MongoStorage schema version {1}. Please update to the latest HangFire.SqlServer NuGet package.",
                        schema.Version, RequiredSchemaVersion));
            }
            else
                AsyncHelper.RunSync(() => Schema.InsertOneAsync(new SchemaDto { Version = RequiredSchemaVersion }));
        }

        /// <summary>
        /// Disposes the object
        /// </summary>
        public void Dispose()
        {
        }
    }
}