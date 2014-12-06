using System;
using System.Collections.Generic;
using System.Linq;
using Hangfire.Mongo.Tests.Utils;
using Hangfire.Server;
using Hangfire.Storage;
using Xunit;

namespace Hangfire.Mongo.Tests
{
	public class MongoStorageFacts
	{
		[Fact]
		public void Ctor_ThrowsAnException_WhenConnectionStringIsNull()
		{
			var exception = Assert.Throws<ArgumentNullException>(() => new MongoStorage(null, "database"));

			Assert.Equal("connectionString", exception.ParamName);
		}

		[Fact]
		public void Ctor_ThrowsAnException_WhenDatabaseNameIsNull()
		{
			var exception = Assert.Throws<ArgumentNullException>(() => new MongoStorage("localhost", null));

			Assert.Equal("databaseName", exception.ParamName);
		}

		[Fact]
		public void Ctor_ThrowsAnException_WhenOptionsValueIsNull()
		{
			var exception = Assert.Throws<ArgumentNullException>(() => new MongoStorage("localhost", "database", null));

			Assert.Equal("options", exception.ParamName);
		}

		[Fact, CleanDatabase]
		public void GetMonitoringApi_ReturnsNonNullInstance()
		{
			MongoStorage storage = CreateStorage();
			IMonitoringApi api = storage.GetMonitoringApi();
			Assert.NotNull(api);
		}

		[Fact, CleanDatabase]
		public void GetConnection_ReturnsNonNullInstance()
		{
			MongoStorage storage = CreateStorage();
			using (IStorageConnection connection = storage.GetConnection())
			{
				Assert.NotNull(connection);
			}
		}

		[Fact]
		public void GetComponents_ReturnsAllNeededComponents()
		{
			MongoStorage storage = CreateStorage();

			IEnumerable<IServerComponent> components = storage.GetComponents();

			Type[] componentTypes = components.Select(x => x.GetType()).ToArray();
			Assert.Contains(typeof(ExpirationManager), componentTypes);
		}

		private static MongoStorage CreateStorage()
		{
			return new MongoStorage(ConnectionUtils.GetConnectionString(), ConnectionUtils.GetDatabaseName());
		}
	}
}