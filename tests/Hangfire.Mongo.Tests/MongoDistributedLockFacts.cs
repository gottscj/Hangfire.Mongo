using System;
using System.Threading;
using Hangfire.Mongo.Database;
using Hangfire.Mongo.Dto;
using Hangfire.Mongo.MongoUtils;
using Hangfire.Mongo.Tests.Utils;
using MongoDB.Driver.Builders;
using Xunit;
using Hangfire.Mongo.DistributedLock;

namespace Hangfire.Mongo.Tests
{
	public class MongoDistributedLockFacts
	{
		[Fact]
		public void Ctor_ThrowsAnException_WhenResourceIsNull()
		{
			UseConnection(database =>
			{
				var exception = Assert.Throws<ArgumentNullException>(
					() => new MongoDistributedLock(null, TimeSpan.FromSeconds(1), database, new MongoStorageOptions()));

				Assert.Equal("resource", exception.ParamName);
			});
		}

		[Fact]
		public void Ctor_ThrowsAnException_WhenConnectionIsNull()
		{
			var exception = Assert.Throws<ArgumentNullException>(
				() => new MongoDistributedLock("resource1", TimeSpan.FromSeconds(1), null, new MongoStorageOptions()));

			Assert.Equal("database", exception.ParamName);
		}

		[Fact, CleanDatabase]
		public void Ctor_SetLock_WhenResourceIsNotLocked()
		{
			UseConnection(database =>
			{
				using (MongoDistributedLock @lock = new MongoDistributedLock("resource1", TimeSpan.FromSeconds(1), database, new MongoStorageOptions()))
				{
					var locksCount = database.DistributedLock.Count(Query<DistributedLockDto>.EQ(_ => _.Resource, "resource1"));
					Assert.Equal(1, locksCount);
				}
			});
		}

		[Fact, CleanDatabase]
		public void Ctor_SetReleaseLock_WhenResourceIsNotLocked()
		{
			UseConnection(database =>
			{
				long locksCount;
				using (MongoDistributedLock @lock = new MongoDistributedLock("resource1", TimeSpan.FromSeconds(1), database, new MongoStorageOptions()))
				{
					locksCount = database.DistributedLock.Count(Query<DistributedLockDto>.EQ(_ => _.Resource, "resource1"));
					Assert.Equal(1, locksCount);
				}

				locksCount = database.DistributedLock.Count(Query<DistributedLockDto>.EQ(_ => _.Resource, "resource1"));
				Assert.Equal(0, locksCount);
			});
		}

		[Fact, CleanDatabase]
		public void Ctor_ThrowsAnException_WhenResourceIsLocked()
		{
			UseConnection(database =>
			{
				using (MongoDistributedLock @lock1 = new MongoDistributedLock("resource1", TimeSpan.FromSeconds(1), database, new MongoStorageOptions()))
				{
					var locksCount = database.DistributedLock.Count(Query<DistributedLockDto>.EQ(_ => _.Resource, "resource1"));
					Assert.Equal(1, locksCount);

					Assert.Throws<MongoDistributedLockException>(() => new MongoDistributedLock("resource1", TimeSpan.FromSeconds(1), database, new MongoStorageOptions()));
				}
			});
		}

		[Fact]
		public void Ctor_ThrowsAnException_WhenOptionsIsNull()
		{
			UseConnection(database =>
			{
				var exception = Assert.Throws<ArgumentNullException>(
					() => new MongoDistributedLock("resource1", TimeSpan.FromSeconds(1), database, null));

				Assert.Equal("options", exception.ParamName);
			});
		}

		[Fact, CleanDatabase]
		public void Ctor_SetLockHeartbeatWorks_WhenResourceIsNotLocked()
		{
			UseConnection(database =>
			{
				using (MongoDistributedLock @lock = new MongoDistributedLock("resource1", TimeSpan.FromSeconds(1), database, new MongoStorageOptions() { DistributedLockLifetime = TimeSpan.FromSeconds(10)}))
				{
					DateTime initialHeartBeat = database.GetServerTimeUtc();
					Thread.Sleep(TimeSpan.FromSeconds(3));
					Thread.Sleep(TimeSpan.FromSeconds(60));

					DistributedLockDto lockEntry = database.DistributedLock.FindOne(Query<DistributedLockDto>.EQ(_ => _.Resource, "resource1"));
					Assert.NotNull(lockEntry);
					Assert.True(lockEntry.Heartbeat > initialHeartBeat);
				}
			});
		}

		private static void UseConnection(Action<HangfireDbContext> action)
		{
			using (var connection = ConnectionUtils.CreateConnection())
			{
				action(connection);
			}
		}
	}
}