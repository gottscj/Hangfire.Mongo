using System;
using Hangfire.Mongo.Database;

namespace Hangfire.Mongo.Tests.Utils
{
	public class ConnectionUtils
	{
		private const string DatabaseVariable = "Hangfire_Mongo_DatabaseName";
		private const string ConnectionStringTemplateVariable = "Hangfire_Mongo_ConnectionStringTemplate";

		private const string DefaultDatabaseName = @"Hangfire-Mongo-Tests";
		private const string DefaultConnectionStringTemplate = @"mongodb://localhost";

		public static string GetDatabaseName()
		{
			return Environment.GetEnvironmentVariable(DatabaseVariable) ?? DefaultDatabaseName;
		}

		public static string GetConnectionString()
		{
			return String.Format(GetConnectionStringTemplate(), GetDatabaseName());
		}

		private static string GetConnectionStringTemplate()
		{
			return Environment.GetEnvironmentVariable(ConnectionStringTemplateVariable) ?? DefaultConnectionStringTemplate;
		}

		public static HangfireDbContext CreateConnection()
		{
			HangfireDbContext connection = new HangfireDbContext(GetConnectionString(), GetDatabaseName());
			return connection;
		}
	}
}