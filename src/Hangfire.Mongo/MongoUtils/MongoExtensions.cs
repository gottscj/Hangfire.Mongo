using System;
using System.Collections.Generic;
using Hangfire.Mongo.Database;
using MongoDB.Bson;
using MongoDB.Driver;

namespace Hangfire.Mongo.MongoUtils
{
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
        public static DateTime GetServerTimeUtc(this IMongoDatabase database)
        {
            dynamic serverStatus = database.RunCommand<dynamic>(new BsonDocument("isMaster", 1));
            object localTime;
            if (((IDictionary<string, object>)serverStatus).TryGetValue("localTime", out localTime))
            {
                return ((DateTime)localTime).ToUniversalTime();
            }
            return DateTime.UtcNow;
        }

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