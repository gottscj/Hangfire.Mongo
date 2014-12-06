using Hangfire.Mongo.Database;
using Hangfire.Mongo.Dto;
using Hangfire.Mongo.MongoUtils;
using Hangfire.Mongo.Tests.Utils;
using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Driver;
using MongoDB.Driver.Builders;
using System;
using System.Threading;
using Xunit;

namespace Hangfire.Mongo.Tests
{
	public class ExpirationManagerFacts
	{
		private readonly MongoStorage _storage;

		private readonly CancellationToken _token;

		public ExpirationManagerFacts()
		{
			_storage = new MongoStorage(ConnectionUtils.GetConnectionString(), ConnectionUtils.GetDatabaseName());
			_token = new CancellationToken(true);
		}

		[Fact]
		public void Ctor_ThrowsAnException_WhenStorageIsNull()
		{
			Assert.Throws<ArgumentNullException>(() => new ExpirationManager(null));
		}

		[Fact, CleanDatabase]
		public void Execute_RemovesOutdatedRecords()
		{
			using (var connection = ConnectionUtils.CreateConnection())
			{
				var entryId = CreateExpirationEntry(connection, connection.GetServerTimeUtc().AddMonths(-1));
				var manager = CreateManager();

				manager.Execute(_token);

				Assert.True(IsEntryExpired(connection, entryId));
			}
		}

		[Fact, CleanDatabase]
		public void Execute_DoesNotRemoveEntries_WithNoExpirationTimeSet()
		{
			using (var connection = ConnectionUtils.CreateConnection())
			{
				var entryId = CreateExpirationEntry(connection, null);
				var manager = CreateManager();

				manager.Execute(_token);

				Assert.False(IsEntryExpired(connection, entryId));
			}
		}

		[Fact, CleanDatabase]
		public void Execute_DoesNotRemoveEntries_WithFreshExpirationTime()
		{
			using (var connection = ConnectionUtils.CreateConnection())
			{
				var entryId = CreateExpirationEntry(connection, DateTime.Now.AddMonths(1));
				var manager = CreateManager();

				manager.Execute(_token);

				Assert.False(IsEntryExpired(connection, entryId));
			}
		}

		[Fact, CleanDatabase]
		public void Execute_Processes_CounterTable()
		{
			using (var connection = ConnectionUtils.CreateConnection())
			{
				// Arrange
				connection.Counter.Insert(new CounterDto
				{
					Id = ObjectId.GenerateNewId(),
					Key = "key",
					Value = 1,
					ExpireAt = connection.GetServerTimeUtc().AddMonths(-1)
				});

				var manager = CreateManager();

				// Act
				manager.Execute(_token);

				// Assert
				var count = connection.Counter.Count();
				Assert.Equal(0, count);
			}
		}

		[Fact, CleanDatabase]
		public void Execute_Processes_JobTable()
		{
			using (var connection = ConnectionUtils.CreateConnection())
			{
				// Arrange
				connection.Job.Insert(new JobDto
				{
					Id = 1,
					InvocationData = "",
					Arguments = "",
					CreatedAt = connection.GetServerTimeUtc(),
					ExpireAt = connection.GetServerTimeUtc().AddMonths(-1),
				});

				var manager = CreateManager();

				// Act
				manager.Execute(_token);

				// Assert
				var count = connection.Job.Count();
				Assert.Equal(0, count);
			}
		}

		[Fact, CleanDatabase]
		public void Execute_Processes_ListTable()
		{
			using (var connection = ConnectionUtils.CreateConnection())
			{
				// Arrange
				connection.List.Insert(new ListDto
				{
					Id = ObjectId.GenerateNewId(),
					Key = "key",
					ExpireAt = connection.GetServerTimeUtc().AddMonths(-1)
				});

				var manager = CreateManager();

				// Act
				manager.Execute(_token);

				// Assert
				var count = connection.List.Count();
				Assert.Equal(0, count);
			}
		}

		[Fact, CleanDatabase]
		public void Execute_Processes_SetTable()
		{
			using (var connection = ConnectionUtils.CreateConnection())
			{
				// Arrange
				connection.Set.Insert(new SetDto
				{
					Id = ObjectId.GenerateNewId(),
					Key = "key",
					Score = 0,
					Value = "",
					ExpireAt = connection.GetServerTimeUtc().AddMonths(-1)
				});

				var manager = CreateManager();

				// Act
				manager.Execute(_token);

				// Assert
				var count = connection.Set.Count();
				Assert.Equal(0, count);
			}
		}

		[Fact, CleanDatabase]
		public void Execute_Processes_HashTable()
		{
			using (var connection = ConnectionUtils.CreateConnection())
			{
				// Arrange
				connection.Hash.Insert(new HashDto
				{
					Id = ObjectId.GenerateNewId(),
					Key = "key",
					Field = "field",
					Value = "",
					ExpireAt = connection.GetServerTimeUtc().AddMonths(-1)
				});

				var manager = CreateManager();

				// Act
				manager.Execute(_token);

				// Assert
				var count = connection.Hash.Count();
				Assert.Equal(0, count);
			}
		}


		private static ObjectId CreateExpirationEntry(HangfireDbContext connection, DateTime? expireAt)
		{
			var counter = new CounterDto
			{
				Id = ObjectId.GenerateNewId(),
				Key = "key",
				Value = 1,
				ExpireAt = expireAt
			};
			connection.Counter.Insert(counter);

			var id = counter.Id;

			return id;
		}

		private static bool IsEntryExpired(HangfireDbContext connection, ObjectId entryId)
		{
			var count = connection.Counter.Count(Query<CounterDto>.EQ(_ => _.Id, entryId));
			return count == 0;
		}

		private ExpirationManager CreateManager()
		{
			return new ExpirationManager(_storage);
		}
	}
}