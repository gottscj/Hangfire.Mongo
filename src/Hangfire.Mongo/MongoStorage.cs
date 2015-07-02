using System.Text.RegularExpressions;
using Hangfire.Logging;
using Hangfire.Mongo.Database;
using Hangfire.Mongo.PersistentJobQueue;
using Hangfire.Mongo.PersistentJobQueue.Mongo;
using Hangfire.Mongo.StateHandlers;
using Hangfire.Server;
using Hangfire.States;
using Hangfire.Storage;
using System;
using System.Collections.Generic;

namespace Hangfire.Mongo
{
	public class MongoStorage : JobStorage
	{
		private readonly Regex _connectionStringRegex = new Regex(@"(?<Unneeded>(.+:(//|\\\\)(.+:.+@)?))(?<ServerName>.+):(?<Port>\d+)", RegexOptions.Compiled);

		private readonly string _connectionString;

		private readonly string _databaseName;

		private readonly MongoStorageOptions _options;

		private string _serverName;

		private string _port;

		public MongoStorage(string connectionString, string databaseName)
			: this(connectionString, databaseName, new MongoStorageOptions())
		{
		}

		public MongoStorage(string connectionString, string databaseName, MongoStorageOptions options)
		{
			if (String.IsNullOrWhiteSpace(connectionString) == true)
				throw new ArgumentNullException("connectionString");

			if (String.IsNullOrWhiteSpace(databaseName) == true)
				throw new ArgumentNullException("databaseName");

			if (options == null)
				throw new ArgumentNullException("options");

			_connectionString = connectionString;
			_databaseName = databaseName;
			_options = options;

			Connection = new HangfireDbContext(connectionString, databaseName, options.Prefix);
			var defaultQueueProvider = new MongoJobQueueProvider(options);
			QueueProviders = new PersistentJobQueueProviderCollection(defaultQueueProvider);

			ParseConnectionString();
		}

		public HangfireDbContext Connection { get; private set; }

		public PersistentJobQueueProviderCollection QueueProviders { get; private set; }

		public override IMonitoringApi GetMonitoringApi()
		{
			return new MongoMonitoringApi(Connection, QueueProviders);
		}

		public override IStorageConnection GetConnection()
		{
			return new MongoConnection(Connection, _options, QueueProviders);
		}

		public override IEnumerable<IServerComponent> GetComponents()
		{
			yield return new ExpirationManager(this);
		}

		public override IEnumerable<IStateHandler> GetStateHandlers()
		{
			yield return new FailedStateHandler();
			yield return new ProcessingStateHandler();
			yield return new SucceededStateHandler();
			yield return new DeletedStateHandler();
		}

		public override void WriteOptionsToLog(ILog logger)
		{
			logger.Info("Using the following options for Mongo DB job storage:");
			logger.InfoFormat("    Prefix: {0}.", _options.Prefix);
		}

		public HangfireDbContext CreateAndOpenConnection()
		{
			return new HangfireDbContext(_connectionString, _databaseName, _options.Prefix);
		}

		public override string ToString()
		{
			return String.Format("Server: {0} Port: {1} Database name: {2}", _serverName, _port, _databaseName);
		}

		private void ParseConnectionString()
		{
			var connectionStringParts = _connectionStringRegex.Match(_connectionString);

			if (connectionStringParts.Groups["ServerName"].Success)
				_serverName = connectionStringParts.Groups["ServerName"].Value;

			if (connectionStringParts.Groups["Port"].Success)
				_port = connectionStringParts.Groups["Port"].Value;
		}
	}
}