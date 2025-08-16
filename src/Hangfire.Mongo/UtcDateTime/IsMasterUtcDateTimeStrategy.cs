using System;
using Hangfire.Mongo.Database;
using MongoDB.Bson;

namespace Hangfire.Mongo.UtcDateTime
{
    /// <summary>
    /// Uses the isMaster command to retrieve server time.
    /// </summary>
    public sealed class IsMasterUtcDateTimeStrategy : UtcDateTimeStrategy
    {
        /// <summary>
        /// Obtain current UTC time using the isMaster command.
        /// </summary>
        /// <param name="dbContext">MongoDB context.</param>
        /// <returns>The UTC time reported by MongoDB.</returns>
        public override DateTime GetUtcDateTime(HangfireDbContext dbContext)
        {
            var isMaster = dbContext.Database.RunCommand<BsonDocument>(new BsonDocument("isMaster", 1));
            return isMaster["localTime"].ToUniversalTime();
        }
    }
}
