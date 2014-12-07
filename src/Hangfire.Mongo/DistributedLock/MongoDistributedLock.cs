using Common.Logging;
using Hangfire.Mongo.Database;
using Hangfire.Mongo.Dto;
using Hangfire.Mongo.MongoUtils;
using MongoDB.Driver;
using MongoDB.Driver.Builders;
using System;
using System.Threading;

namespace Hangfire.Mongo.DistributedLock
{
	public class MongoDistributedLock : IDisposable
	{
		private static readonly ILog Logger = LogManager.GetLogger(typeof(MongoDistributedLock));

		private readonly HangfireDbContext _database;

		private readonly MongoStorageOptions _options;

		private readonly string _resource;

		private Timer _heartbeatTimer = null;

		private bool _completed;

		public MongoDistributedLock(string resource, TimeSpan timeout, HangfireDbContext database, MongoStorageOptions options)
		{
			if (String.IsNullOrEmpty(resource) == true)
				throw new ArgumentNullException("resource");

			if (database == null)
				throw new ArgumentNullException("database");

			if (options == null)
				throw new ArgumentNullException("options");

			_resource = resource;
			_database = database;
			_options = options;

			try
			{
				// Remove dead locks
				database.DistributedLock.Remove(Query.And(Query<DistributedLockDto>.EQ(_ => _.Resource, resource),
					Query<DistributedLockDto>.LT(_ => _.Heartbeat, database.GetServerTimeUtc().Subtract(options.DistributedLockLifetime))));

				// Check lock
				DateTime lockTimeoutTime = DateTime.Now.Add(timeout);
				bool isLockedBySomeoneElse;
				bool isFirstAttempt = true;
				do
				{
					isLockedBySomeoneElse = database
						.DistributedLock
						.FindOne(Query.And(Query<DistributedLockDto>.EQ(_ => _.Resource, resource),
							Query<DistributedLockDto>.NE(_ => _.ClientId, _options.ClientId))) != null;

					if (isFirstAttempt == true)
						isFirstAttempt = false;
					else
						Thread.Sleep((int)timeout.TotalMilliseconds / 10);
				}
				while ((isLockedBySomeoneElse == true) && (lockTimeoutTime >= DateTime.Now));

				// Set lock
				if (isLockedBySomeoneElse == false)
				{
					database.DistributedLock.FindAndModify(new FindAndModifyArgs
					{
						Query = Query<DistributedLockDto>.EQ(_ => _.Resource, resource),
						Update = Update.Combine(
							Update<DistributedLockDto>.Set(_ => _.ClientId, _options.ClientId),
							Update<DistributedLockDto>.Inc(_ => _.LockCount, 1),
							Update<DistributedLockDto>.Set(_ => _.Heartbeat, database.GetServerTimeUtc())
							),
						Upsert = true
					});

					StartHeartBeat();
				}
				else
				{
					throw new MongoDistributedLockException(String.Format("Could not place a lock on the resource '{0}': {1}.", _resource, "The lock request timed out"));
				}
			}
			catch (Exception ex)
			{
				if (ex is MongoDistributedLockException)
					throw;
				else
					throw new MongoDistributedLockException(String.Format("Could not place a lock on the resource '{0}': {1}.", _resource, "Check inner exception for details"), ex);
			}
		}

		public void Dispose()
		{
			if (_completed)
				return;

			try
			{
				// Remove dead locks
				_database.DistributedLock.Remove(Query.And(Query<DistributedLockDto>.EQ(_ => _.Resource, _resource),
					Query<DistributedLockDto>.LT(_ => _.Heartbeat, _database.GetServerTimeUtc().Subtract(_options.DistributedLockLifetime))));

				// Remove resource lock
				_database.DistributedLock.FindAndModify(new FindAndModifyArgs
				{
					Query = Query.And(Query<DistributedLockDto>.EQ(_ => _.Resource, _resource),
					Query<DistributedLockDto>.EQ(_ => _.ClientId, _options.ClientId)),
					Update = Update.Combine(
						Update<DistributedLockDto>.Inc(_ => _.LockCount, -1),
						Update<DistributedLockDto>.Set(_ => _.Heartbeat, _database.GetServerTimeUtc())
						),
					Upsert = false
				});

				_database.DistributedLock.Remove(Query<DistributedLockDto>.LTE(_ => _.LockCount, 0));

				if (_heartbeatTimer != null)
				{
					_heartbeatTimer.Dispose();
					_heartbeatTimer = null;
				}

				_completed = true;
			}
			catch (Exception ex)
			{
				throw new MongoDistributedLockException(String.Format("Could not release a lock on the resource '{0}': {1}.", _resource, "Check inner exception for details"), ex);
			}
		}

		private void StartHeartBeat()
		{
			TimeSpan timerInterval = TimeSpan.FromMilliseconds(_options.DistributedLockLifetime.TotalMilliseconds / 5);

			_heartbeatTimer = new Timer(state =>
			{
				try
				{
					_database.DistributedLock.Update(Query.And(Query<DistributedLockDto>.EQ(_ => _.Resource, _resource), Query<DistributedLockDto>.EQ(_ => _.ClientId, _options.ClientId)),
								Update<DistributedLockDto>.Set(_ => _.Heartbeat, _database.GetServerTimeUtc()));
				}
				catch (Exception ex)
				{
					Logger.ErrorFormat("Unable to update heartbeat on the resource '{0}'", ex, _resource);
				}
			}, null, timerInterval, timerInterval);
		}
	}
}