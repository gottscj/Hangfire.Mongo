using System;
using System.Collections.Generic;
using System.Linq;
using Hangfire.Logging;
using Hangfire.Mongo.Database;
using Hangfire.Mongo.Migration;
using Hangfire.Server;
using Hangfire.Storage;
using MongoDB.Driver;

namespace Hangfire.Mongo
{
    /// <summary>
    /// Hangfire Job Storage implementation for Mongo database
    /// </summary>
    public class MongoStorage : JobStorage
    {
        private readonly string _databaseName;

        private readonly MongoClientSettings _mongoClientSettings;

        private readonly MongoStorageOptions _storageOptions;

        private readonly HangfireDbContext _dbContext;

        /// <summary>
        /// Constructs Job Storage by database connection string and name
        /// </summary>
        /// <param name="connectionString">MongoDB connection string</param>
        /// <param name="databaseName">Database name</param>
        [Obsolete("Please use overload which takes a 'MongoClientSettings' object")]
        public MongoStorage(string connectionString, string databaseName)
            : this(connectionString, databaseName, new MongoStorageOptions())
        {
        }

        /// <summary>
        /// Constructs Job Storage by database connection string, name and options
        /// </summary>
        /// <param name="connectionString">MongoDB connection string</param>
        /// <param name="databaseName">Database name</param>
        /// <param name="storageOptions">Storage options</param>
        [Obsolete("Please use overload which takes a 'MongoClientSettings' object")]
        public MongoStorage(string connectionString, string databaseName, MongoStorageOptions storageOptions)
            : this(MongoClientSettings.FromConnectionString(connectionString),databaseName, storageOptions)
        {
            
        }

        /// <summary>
        /// Constructs Job Storage by Mongo client settings and name
        /// </summary>
        /// <param name="mongoClientSettings">Client settings for MongoDB</param>
        /// <param name="databaseName">Database name</param>
        public MongoStorage(MongoClientSettings mongoClientSettings, string databaseName)
            : this(mongoClientSettings, databaseName, new MongoStorageOptions())
        {
        }

        /// <summary>
        /// Constructs Job Storage by Mongo client settings, name and options
        /// </summary>
        /// <param name="mongoClientSettings">Client settings for MongoDB</param>
        /// <param name="databaseName">Database name</param>
        /// <param name="storageOptions">Storage options</param>
        public MongoStorage(MongoClientSettings mongoClientSettings, string databaseName, MongoStorageOptions storageOptions)
        {
            if (mongoClientSettings == null)
            {
                throw new ArgumentNullException(nameof(mongoClientSettings));
            }
            if (string.IsNullOrWhiteSpace(databaseName))
            {
                throw new ArgumentNullException(nameof(databaseName));
            }
            if (storageOptions == null)
            {
                throw new ArgumentNullException(nameof(storageOptions));
            }

            _mongoClientSettings = mongoClientSettings;
            _databaseName = databaseName;
            _storageOptions = storageOptions;

            _dbContext = new HangfireDbContext(mongoClientSettings, databaseName, _storageOptions.Prefix);
            
            using (var migrationManager = new MongoMigrationManager(storageOptions, _dbContext))
            {
                
                migrationManager.Migrate();
            }
        }

        /// <summary>
        /// Returns Monitoring API object
        /// </summary>
        /// <returns>Monitoring API object</returns>
        public override IMonitoringApi GetMonitoringApi()
        {
            return new MongoMonitoringApi(_dbContext);
        }

        /// <summary>
        /// Returns storage connection
        /// </summary>
        /// <returns>Storage connection</returns>
        public override IStorageConnection GetConnection()
        {
            return new MongoConnection(_dbContext, _storageOptions);
        }

        /// <summary>
        /// Returns collection of server components
        /// </summary>
        /// <returns>Collection of server components</returns>
        public override IEnumerable<IServerComponent> GetComponents()
        {
            yield return new ExpirationManager(_dbContext, _storageOptions.JobExpirationCheckInterval);
        }

        /// <summary>
        /// Writes storage options to log
        /// </summary>
        /// <param name="logger">Logger</param>
        public override void WriteOptionsToLog(ILog logger)
        {
            logger.Info("Using the following options for Mongo DB job storage:");
            logger.InfoFormat("    Prefix: {0}.", _storageOptions.Prefix);
        }

        /// <summary>
        /// Returns text representation of the object
        /// </summary>
        public override string ToString()
        {
            // Obscure the username and password for display purposes
            string obscuredConnectionString = "mongodb://";
            if (_mongoClientSettings != null && _mongoClientSettings.Servers != null)
            {
                var servers = string.Join(",", _mongoClientSettings.Servers.Select(s => $"{s.Host}:{s.Port}"));
                obscuredConnectionString = $"mongodb://<username>:<password>@{servers}";
            }
            return $"Connection string: {obscuredConnectionString}, database name: {_databaseName}, prefix: {_storageOptions.Prefix}";
        }
    }
}