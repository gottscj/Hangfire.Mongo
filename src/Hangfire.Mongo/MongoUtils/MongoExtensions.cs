using Hangfire.Mongo.Database;
using MongoDB.Bson;
using MongoDB.Driver;
using System;

namespace Hangfire.Mongo.MongoUtils
{
	public static class MongoExtensions
	{
		public static DateTime GetServerTimeUtc(this MongoDatabase database)
		{
			return database.Eval(new EvalArgs
			{
				Code = new BsonJavaScript("new Date()")
			}).ToUniversalTime();
		}

		public static DateTime GetServerTimeUtc(this HangfireDbContext dbContext)
		{
			return GetServerTimeUtc(dbContext.Database);
		}
	}
}
