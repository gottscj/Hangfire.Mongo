using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text.RegularExpressions;
using Hangfire.Logging;
using Hangfire.Mongo.Database;
using Hangfire.Mongo.PersistentJobQueue;
using Hangfire.Mongo.PersistentJobQueue.Mongo;
using Hangfire.Server;
using Hangfire.States;
using Hangfire.Storage;
using MongoDB.Bson.Serialization;
using MongoDB.Bson.Serialization.Conventions;
using MongoDB.Driver;

namespace Hangfire.Mongo
{
    /// <summary>
    /// Hangfire Job Storage implementation for Mongo database
    /// </summary>
    public class MongoStorage : JobStorage
    {
        private readonly string _connectionString;

        private static readonly Regex ConnectionStringCredentials = new Regex("mongodb://(.*?)@", RegexOptions.Compiled | RegexOptions.CultureInvariant);

        private readonly string _databaseName;

        private readonly MongoClientSettings _mongoClientSettings;

        private readonly MongoStorageOptions _storageOptions;


        static MongoStorage()
        {
            // We will register all our Dto classes with the default conventions.
            // By doing this, we can safely use strings for referencing class
            // property names with risking to have a mismatch with any convention
            // used by bson serializer.
            var conventionPack = new ConventionPack();
            conventionPack.Append(DefaultConventionPack.Instance);
            conventionPack.Append(AttributeConventionPack.Instance);
            var conventionRunner = new ConventionRunner(conventionPack);

            var assembly = typeof(MongoStorage).GetTypeInfo().Assembly;
            var classMaps = assembly.DefinedTypes
                .Where(t => t.IsClass && !t.IsAbstract && !t.IsGenericType && t.Namespace == "Hangfire.Mongo.Dto")
                .Select(t => new BsonClassMap(t.AsType()));

            foreach (var classMap in classMaps)
            {
                conventionRunner.Apply(classMap);
                BsonClassMap.RegisterClassMap(classMap);
            }
        }


        /// <summary>
        /// Constructs Job Storage by database connection string and name
        /// </summary>
        /// <param name="connectionString">MongoDB connection string</param>
        /// <param name="databaseName">Database name</param>
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
        public MongoStorage(string connectionString, string databaseName, MongoStorageOptions storageOptions)
        {
            if (string.IsNullOrWhiteSpace(connectionString))
            {
                throw new ArgumentNullException(nameof(connectionString));
            }
            if (string.IsNullOrWhiteSpace(databaseName))
            {
                throw new ArgumentNullException(nameof(databaseName));
            }
            if (storageOptions == null)
            {
                throw new ArgumentNullException(nameof(storageOptions));
            }

            _connectionString = connectionString;
            _databaseName = databaseName;
            _storageOptions = storageOptions;

            Connection = new HangfireDbContext(connectionString, databaseName, storageOptions.Prefix);
            Connection.Init(_storageOptions);

            var defaultQueueProvider = new MongoJobQueueProvider(_storageOptions);
            QueueProviders = new PersistentJobQueueProviderCollection(defaultQueueProvider);
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

            Connection = new HangfireDbContext(mongoClientSettings, databaseName, _storageOptions.Prefix);

            var defaultQueueProvider = new MongoJobQueueProvider(_storageOptions);
            QueueProviders = new PersistentJobQueueProviderCollection(defaultQueueProvider);
        }

        /// <summary>
        /// Database context
        /// </summary>
        [Obsolete("We are removing access to HangfireDbContext in 0.5.13, please open an issue on our github page if you need this functionality")]
        public HangfireDbContext Connection { get; }

        /// <summary>
        /// Queue providers collection
        /// </summary>
        [Obsolete("We are removing support for external queue providers in 0.5.13")]
        public PersistentJobQueueProviderCollection QueueProviders { get; }

        /// <summary>
        /// Returns Monitoring API object
        /// </summary>
        /// <returns>Monitoring API object</returns>
        public override IMonitoringApi GetMonitoringApi()
        {
            return new MongoMonitoringApi(Connection, QueueProviders);
        }

        /// <summary>
        /// Returns storage connection
        /// </summary>
        /// <returns>Storage connection</returns>
        public override IStorageConnection GetConnection()
        {
            return new MongoConnection(Connection, _storageOptions, QueueProviders);
        }

        /// <summary>
        /// Returns collection of server components
        /// </summary>
        /// <returns>Collection of server components</returns>
        public override IEnumerable<IServerComponent> GetComponents()
        {
            yield return new ExpirationManager(this, _storageOptions.JobExpirationCheckInterval);
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
        /// Opens connection to database
        /// </summary>
        /// <returns>Database context</returns>
        [Obsolete("We are removing access to HangfireDbContext in 0.5.13, please open an issue on our github page if you need this functionality")]
        public HangfireDbContext CreateAndOpenConnection()
        {
            return _connectionString != null
                ? new HangfireDbContext(_connectionString, _databaseName, _storageOptions.Prefix)
                : new HangfireDbContext(_mongoClientSettings, _databaseName, _storageOptions.Prefix);
        }

        /// <summary>
        /// Returns text representation of the object
        /// </summary>
        public override string ToString()
        {
            // Obscure the username and password for display purposes
            string obscuredConnectionString = "mongodb://";
            if (_connectionString != null)
            {
                obscuredConnectionString = ConnectionStringCredentials.Replace(_connectionString, "mongodb://<username>:<password>@");
            }
            else if (_mongoClientSettings != null && _mongoClientSettings.Servers != null)
            {
                var servers = string.Join(",", _mongoClientSettings.Servers.Select(s => $"{s.Host}:{s.Port}"));
                obscuredConnectionString = $"mongodb://<username>:<password>@{servers}";
            }
            return $"Connection string: {obscuredConnectionString}, database name: {_databaseName}, prefix: {_storageOptions.Prefix}";
        }
    }
}