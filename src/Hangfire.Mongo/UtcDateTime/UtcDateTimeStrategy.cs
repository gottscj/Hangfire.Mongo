using System;
using Hangfire.Logging;
using Hangfire.Mongo.Database;

namespace Hangfire.Mongo.UtcDateTime
{
    /// <summary>
    /// Strategy for obtaining UTC time from MongoDB server
    /// </summary>
    public abstract class UtcDateTimeStrategy
    {
        /// <summary>
        /// Obtain current UTC time using the provided MongoDB context.
        /// </summary>
        /// <param name="dbContext">MongoDB context.</param>
        /// <param name="logger">Logger instance.</param>
        /// <returns>UTC DateTime.</returns>
        public abstract DateTime GetUtcDateTime(HangfireDbContext dbContext, ILog logger);
    }
}
