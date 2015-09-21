using Hangfire.Mongo.Database;
using MongoDB.Driver;
using System;

namespace Hangfire.Mongo.MongoUtils
{
	public static class MongoExtensions
	{
		public static DateTime GetServerTimeUtc(this MongoDatabase database)
		{
			return database.RunCommand("isMaster")
				.Response
				.AsBsonDocument["localTime"]
				.ToUniversalTime();
		}

		public static DateTime GetServerTimeUtc(this HangfireDbContext dbContext)
		{
			return GetServerTimeUtc(dbContext.Database);
		}
	}
}
