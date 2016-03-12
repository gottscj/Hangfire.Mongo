using System;
using System.Collections.Generic;
using System.Text.RegularExpressions;
using Hangfire.Logging;
using Hangfire.Mongo.Database;
using Hangfire.Mongo.PersistentJobQueue;
using Hangfire.Mongo.PersistentJobQueue.Mongo;
using Hangfire.Mongo.StateHandlers;
using Hangfire.Server;
using Hangfire.States;
using Hangfire.Storage;
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

        private readonly MongoStorageOptions _options;

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
        /// <param name="options">Storage options</param>
        public MongoStorage(string connectionString, string databaseName, MongoStorageOptions options)
        {
            if (string.IsNullOrWhiteSpace(connectionString))
                throw new ArgumentNullException("connectionString");

            if (string.IsNullOrWhiteSpace(databaseName))
                throw new ArgumentNullException("databaseName");

            if (options == null)
                throw new ArgumentNullException("options");

            _connectionString = connectionString;
            _databaseName = databaseName;
            _options = options;

            Connection = new HangfireDbContext(connectionString, databaseName, options.Prefix);
            var defaultQueueProvider = new MongoJobQueueProvider(options);
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
		/// <param name="options">Storage options</param>
		public MongoStorage(MongoClientSettings mongoClientSettings, string databaseName, MongoStorageOptions options)
		{
			if (mongoClientSettings == null)
				throw new ArgumentNullException("mongoClientSettings");

			if (String.IsNullOrWhiteSpace(databaseName) == true)
				throw new ArgumentNullException("databaseName");

			if (options == null)
				throw new ArgumentNullException("options");

			_mongoClientSettings = mongoClientSettings;
			_databaseName = databaseName;
			_options = options;

			Connection = new HangfireDbContext(mongoClientSettings, databaseName, options.Prefix);
			var defaultQueueProvider = new MongoJobQueueProvider(options);
			QueueProviders = new PersistentJobQueueProviderCollection(defaultQueueProvider);
		}

        /// <summary>
        /// Database context
        /// </summary>
        public HangfireDbContext Connection { get; private set; }

        /// <summary>
        /// Queue providers collection
        /// </summary>
        public PersistentJobQueueProviderCollection QueueProviders { get; private set; }

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
            return new MongoConnection(Connection, _options, QueueProviders);
        }

        /// <summary>
        /// Returns collection of server components
        /// </summary>
        /// <returns>Collection of server components</returns>
        public override IEnumerable<IServerComponent> GetComponents()
        {
            yield return new ExpirationManager(this, _options.JobExpirationCheckInterval);
            yield return new CountersAggregator(this, _options.CountersAggregateInterval);
        }

        /// <summary>
        /// Returns collection of state handers
        /// </summary>
        /// <returns>Collection of state handers</returns>
        public override IEnumerable<IStateHandler> GetStateHandlers()
        {
            yield return new FailedStateHandler();
            yield return new ProcessingStateHandler();
            yield return new SucceededStateHandler();
            yield return new DeletedStateHandler();
        }

        /// <summary>
        /// Writes storage options to log
        /// </summary>
        /// <param name="logger">Logger</param>
        public override void WriteOptionsToLog(ILog logger)
        {
            logger.Info("Using the following options for Mongo DB job storage:");
            logger.InfoFormat("    Prefix: {0}.", _options.Prefix);
        }

        /// <summary>
        /// Opens connection to database
        /// </summary>
        /// <returns>Database context</returns>
        public HangfireDbContext CreateAndOpenConnection()
        {
	        return this._connectionString != null ? new HangfireDbContext(this._connectionString, this._databaseName, this._options.Prefix) : new HangfireDbContext(this._mongoClientSettings, this._databaseName, this._options.Prefix);
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
	        else if (_mongoClientSettings != null && _mongoClientSettings.Server != null)
	        {
		        obscuredConnectionString = string.Format("mongodb://<username>:<password>@{0}:{1}", _mongoClientSettings.Server.Host, _mongoClientSettings.Server.Port);
	        }
            return String.Format("Connection string: {0}, database name: {1}, prefix: {2}", obscuredConnectionString, _databaseName, _options.Prefix);
			
        }
    }
}