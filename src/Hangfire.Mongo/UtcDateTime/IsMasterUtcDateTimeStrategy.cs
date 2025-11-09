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
            var localTime = isMaster["localTime"];
            if (localTime.IsInt64)
            {
                var unixDate = localTime.AsInt64;
                var start = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
                var time = start.AddMilliseconds(unixDate).ToUniversalTime();
                return time;
            }
            return isMaster["localTime"].ToUniversalTime();
        }
    }
}
