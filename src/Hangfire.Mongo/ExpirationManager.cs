using Hangfire.Logging;
using Hangfire.Mongo.Database;
using Hangfire.Mongo.MongoUtils;
using Hangfire.Server;
using System;
using System.Threading;
using MongoDB.Driver;
using MongoDB.Driver.Builders;

namespace Hangfire.Mongo
{
	public class ExpirationManager : IServerComponent
	{
	    private static readonly ILog Logger = LogProvider.GetCurrentClassLogger();

		private readonly MongoStorage _storage;
		private readonly TimeSpan _checkInterval;

		public ExpirationManager(MongoStorage storage)
			: this(storage, TimeSpan.FromHours(1))
		{
		}

		public ExpirationManager(MongoStorage storage, TimeSpan checkInterval)
		{
			if (storage == null)
				throw new ArgumentNullException("storage");

			_storage = storage;
			_checkInterval = checkInterval;
		}

		public void Execute(CancellationToken cancellationToken)
		{
			using (HangfireDbContext connection = _storage.CreateAndOpenConnection())
			{
				MongoCollection[] processedTables =
				{
					connection.Counter,
					connection.Job,
					connection.List,
					connection.Set,
					connection.Hash
				};

				DateTime now = connection.GetServerTimeUtc();
				foreach (var table in processedTables)
				{
					Logger.DebugFormat("Removing outdated records from table '{0}'...", table.Name);

					table.Remove(Query.LT("ExpireAt", now));
				}
			}

			cancellationToken.WaitHandle.WaitOne(_checkInterval);
		}

		public override string ToString()
		{
			return "Mongo Expiration Manager";
		}
	}
}