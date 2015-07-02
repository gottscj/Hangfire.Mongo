using Hangfire.Mongo.Database;
using MongoDB.Bson;
using MongoDB.Driver;
using System;

namespace Hangfire.Mongo.MongoUtils
{
<<<<<<< HEAD
    /// <summary>
    /// Helper utilities to work with Mongo database
    /// </summary>
    public static class MongoExtensions
    {
        /// <summary>
        /// Retreives server time in UTC zone
        /// </summary>
        /// <param name="database">Mongo database</param>
        /// <returns>Server time</returns>
        public static DateTime GetServerTimeUtc(this MongoDatabase database)
        {
            return database.Eval(new EvalArgs
            {
                Code = new BsonJavaScript("new Date()")
            }).ToUniversalTime();
        }
=======
	public static class MongoExtensions
	{
		public static DateTime GetServerTimeUtc(this MongoDatabase database)
		{
			return database.RunCommand("serverStatus")
				.Response
				.AsBsonDocument["localTime"]
				.ToUniversalTime();
		}
>>>>>>> origin/master

        /// <summary>
        /// Retreives server time in UTC zone
        /// </summary>
        /// <param name="dbContext">Hangfire database context</param>
        /// <returns>Server time</returns>
        public static DateTime GetServerTimeUtc(this HangfireDbContext dbContext)
        {
            return GetServerTimeUtc(dbContext.Database);
        }
    }
}
