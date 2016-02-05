using Hangfire.Mongo.Database;
using MongoDB.Driver;
using System;
using MongoDB.Bson;

namespace Hangfire.Mongo.MongoUtils
{
    /// <summary>
    /// Helper utilities to work with Mongo database
    /// </summary>
    public static class MongoExtensions
    {
        /// <summary>
        /// Retrieves server time in UTC zone
        /// </summary>
        /// <param name="database">Mongo database</param>
        /// <returns>Server time</returns>
        public static DateTime GetServerTimeUtc(this IMongoDatabase database)
        {
            try
            {
                dynamic serverStatus = database.RunCommand<dynamic>(new BsonDocument("isMaster", 1));
                return ((DateTime)serverStatus.localTime).ToUniversalTime();
            }
            catch (MongoException)
            {
                return DateTime.UtcNow;
            }
        }

        /// <summary>
        /// Retrieves server time in UTC zone
        /// </summary>
        /// <param name="dbContext">Hangfire database context</param>
        /// <returns>Server time</returns>
        public static DateTime GetServerTimeUtc(this HangfireDbContext dbContext)
        {
            return GetServerTimeUtc(dbContext.Database);
        }
    }
}