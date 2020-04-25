using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Hangfire.Logging;
using Hangfire.Mongo.Database;
using Hangfire.Mongo.Migration;
using Hangfire.Server;
using Hangfire.Storage;
using MongoDB.Bson;
using MongoDB.Driver;

namespace Hangfire.Mongo
{
    /// <summary>
    /// Hangfire Job Storage implementation for Mongo database
    /// </summary>
    public class MongoStorage : JobStorage
    {
        private readonly string _databaseName;

        private readonly MongoClient _mongoClient;

        private readonly MongoStorageOptions _storageOptions;

        private readonly HangfireDbContext _dbContext;

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
            : this(new MongoClient(mongoClientSettings), databaseName, storageOptions)
        {
            
        }

        /// <summary>
        /// Constructs Job Storage by Mongo client, name and options
        /// </summary>
        /// <param name="mongoClient"></param>
        /// <param name="databaseName"></param>
        /// <param name="storageOptions"></param>
        /// <exception cref="ArgumentNullException"></exception>
        public MongoStorage(MongoClient mongoClient, string databaseName, MongoStorageOptions storageOptions)
        {
            if (string.IsNullOrWhiteSpace(databaseName))
            {
                throw new ArgumentNullException(nameof(databaseName));
            }
            _databaseName = databaseName;
            _mongoClient = mongoClient ?? throw new ArgumentNullException(nameof(mongoClient));
            _storageOptions = storageOptions ?? throw new ArgumentNullException(nameof(storageOptions));
            _dbContext = _storageOptions.Factory.CreateDbContext(mongoClient, databaseName);

            if (_storageOptions.CheckConnection)
            {
                CheckConnection();
            }
             
            MongoMigrationManager.MigrateIfNeeded(storageOptions, _dbContext.Database);
        }

        private void CheckConnection()
        {
            using (var cts = new CancellationTokenSource(_storageOptions.ConnectionCheckTimeout))
            {
                try
                {
                    _dbContext.Database.RunCommand((Command<BsonDocument>)"{ping:1}", cancellationToken: cts.Token);
                }
                catch (Exception e)
                {
                    throw new MongoConnectException(_dbContext, CreateObscuredConnectionString(), e);
                }
            }
        }
        
        /// <summary>
        /// Returns Monitoring API object
        /// </summary>
        /// <returns>Monitoring API object</returns>
        public override IMonitoringApi GetMonitoringApi()
        {
            return _storageOptions.Factory.CreateMongoMonitoringApi(_dbContext);
        }

        /// <summary>
        /// Returns storage connection
        /// </summary>
        /// <returns>Storage connection</returns>
        public override IStorageConnection GetConnection()
        {
            return _storageOptions.Factory.CreateMongoConnection(_dbContext);
        }

        /// <summary>
        /// Returns collection of server components
        /// </summary>
        /// <returns>Collection of server components</returns>
        public override IEnumerable<IServerComponent> GetComponents()
        {
            yield return _storageOptions.Factory.CreateMongoExpirationManager(_dbContext);
            yield return _storageOptions.Factory.CreateMongoNotificationObserver(_dbContext);
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
            
            return $"Connection string: {CreateObscuredConnectionString()}, database name: {_databaseName}, prefix: {_storageOptions.Prefix}";
        }

        private string CreateObscuredConnectionString()
        {
            // Obscure the username and password for display purposes
            string obscuredConnectionString = "mongodb://";
            if (_mongoClient.Settings != null && _mongoClient.Settings.Servers != null)
            {
                var servers = string.Join(",", _mongoClient.Settings.Servers.Select(s => $"{s.Host}:{s.Port}"));
                obscuredConnectionString = $"mongodb://<username>:<password>@{servers}";
            }

            return obscuredConnectionString;
        }
    }
}