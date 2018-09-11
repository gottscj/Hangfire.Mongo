using System;
using Hangfire.Mongo.Dto;
using Hangfire.Mongo.Migration;
using MongoDB.Driver;

namespace Hangfire.Mongo.Database
{
    /// <summary>
    /// Represents Mongo database context for Hangfire
    /// </summary>
    public sealed class HangfireDbContext : IDisposable
    {
        private readonly string _prefix;

        internal MongoClient Client { get; }

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

            Client = new MongoClient(connectionString);

            Database = Client.GetDatabase(databaseName);

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
        /// Mongo database connection identifier
        /// </summary>
        public string ConnectionId { get; private set; }

        /// <summary>
        /// Reference to job graph collection
        /// </summary>
        public IMongoCollection<BaseJobDto> JobGraph => Database.GetCollection<BaseJobDto>(_prefix + ".jobGraph");

        /// <summary>
        /// Reference to collection which contains distributed locks
        /// </summary>
        public IMongoCollection<DistributedLockDto> DistributedLock => Database
            .GetCollection<DistributedLockDto>(_prefix + ".locks");

        /// <summary>
        /// Reference to collection which contains schemas
        /// </summary>
        public IMongoCollection<SchemaDto> Schema => Database.GetCollection<SchemaDto>(_prefix + ".schema");

        /// <summary>
        /// Reference to collection which contains servers information
        /// </summary>
        public IMongoCollection<ServerDto> Server => Database.GetCollection<ServerDto>(_prefix + ".server");

        /// <summary>
        /// Initializes intial collections schema for Hangfire
        /// </summary>
        public void Init(MongoStorageOptions storageOptions)
        {
            var migrationManager = new MongoMigrationManager(storageOptions);
            migrationManager.Migrate(this);
        }


        /// <summary>
        /// Disposes the object
        /// </summary>
        public void Dispose()
        {
        }
    }
}