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
        /// <summary>
        /// Name of hangfire bb
        /// </summary>
        protected readonly string DatabaseName;

        /// <summary>
        /// Mongo client instance used for hangfire mongo
        /// </summary>
        protected readonly IMongoClient MongoClient;

        /// <summary>
        /// Storage options
        /// </summary>
        protected readonly MongoStorageOptions StorageOptions;
        
        /// <summary>
        /// DB context
        /// </summary>
        protected readonly HangfireDbContext HangfireDbContext;

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
        public MongoStorage(IMongoClient mongoClient, string databaseName, MongoStorageOptions storageOptions)
        {
            if (string.IsNullOrWhiteSpace(databaseName))
            {
                throw new ArgumentNullException(nameof(databaseName));
            }
            DatabaseName = databaseName;
            MongoClient = mongoClient ?? throw new ArgumentNullException(nameof(mongoClient));
            StorageOptions = storageOptions ?? throw new ArgumentNullException(nameof(storageOptions));
            HangfireDbContext = StorageOptions.Factory.CreateDbContext(mongoClient, databaseName, storageOptions.Prefix);

            if (StorageOptions.CheckConnection)
            {
                CheckConnection();
            }

            if (!StorageOptions.ByPassMigration)
            {
                MongoMigrationManager.MigrateIfNeeded(storageOptions, HangfireDbContext.Database);
            }
        }
        
        private void CheckConnection()
        {
            using (var cts = new CancellationTokenSource(StorageOptions.ConnectionCheckTimeout))
            {
                try
                {
                    HangfireDbContext.Database.RunCommand((Command<BsonDocument>)"{ping:1}", cancellationToken: cts.Token);
                }
                catch (Exception e)
                {
                    throw new MongoConnectException(HangfireDbContext, CreateObscuredConnectionString(), StorageOptions.ConnectionCheckTimeout, e);
                }
            }
        }
        
        /// <summary>
        /// Returns Monitoring API object
        /// </summary>
        /// <returns>Monitoring API object</returns>
        public override IMonitoringApi GetMonitoringApi()
        {
            return StorageOptions.Factory.CreateMongoMonitoringApi(HangfireDbContext);
        }

        /// <summary>
        /// Returns storage connection
        /// </summary>
        /// <returns>Storage connection</returns>
        public override IStorageConnection GetConnection()
        {
            return StorageOptions.Factory.CreateMongoConnection(HangfireDbContext, StorageOptions);
        }

        /// <summary>
        /// Returns collection of server components
        /// </summary>
        /// <returns>Collection of server components</returns>
        public override IEnumerable<IServerComponent> GetComponents()
        {
            yield return StorageOptions.Factory.CreateMongoExpirationManager(HangfireDbContext, StorageOptions);
            switch (StorageOptions.CheckQueuedJobsStrategy)
            {
                case CheckQueuedJobsStrategy.Watch:
                    yield return StorageOptions.Factory.CreateMongoJobQueueWatcher(HangfireDbContext, StorageOptions);
                    break;
                case CheckQueuedJobsStrategy.TailNotificationsCollection:
                    yield return StorageOptions.Factory.CreateMongoNotificationObserver(HangfireDbContext, StorageOptions);
                    break;
            }
        }

        /// <summary>
        /// Writes storage options to log
        /// </summary>
        /// <param name="logger">Logger</param>
        public override void WriteOptionsToLog(ILog logger)
        {
            logger.Info("Using the following options for Mongo DB job storage:");
            logger.InfoFormat("    Prefix: {0}.", StorageOptions.Prefix);
        }

        /// <summary>
        /// Returns text representation of the object
        /// </summary>
        public override string ToString()
        {
            
            return $"Connection string: {CreateObscuredConnectionString()}, database name: {DatabaseName}, prefix: {StorageOptions.Prefix}";
        }

        private string CreateObscuredConnectionString()
        {
            // Obscure the username and password for display purposes
            string obscuredConnectionString = "mongodb://";
            if (MongoClient.Settings != null && MongoClient.Settings.Servers != null)
            {
                var servers = string.Join(",", MongoClient.Settings.Servers.Select(s => $"{s.Host}:{s.Port}"));
                obscuredConnectionString = $"mongodb://<username>:<password>@{servers}";
            }

            return obscuredConnectionString;
        }
    }
}