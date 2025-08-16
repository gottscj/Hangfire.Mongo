using System;
using Hangfire.Mongo.Database;
using MongoDB.Bson;

namespace Hangfire.Mongo.UtcDateTime
{
    /// <summary>
    /// Uses the serverStatus command to retrieve server time.
    /// </summary>
    public sealed class ServerStatusUtcDateTimeStrategy : UtcDateTimeStrategy
    {
        /// <summary>
        /// Obtain current UTC time using the serverStatus command.
        /// </summary>
        /// <param name="dbContext">MongoDB context.</param>
        /// <returns>The UTC time reported by MongoDB.</returns>
        public override DateTime GetUtcDateTime(HangfireDbContext dbContext)
        {
            var serverStatus = dbContext.Database.RunCommand<BsonDocument>(new BsonDocument("serverStatus", 1));
            return serverStatus["localTime"].ToUniversalTime();
        }
    }
}
