using System;
using System.Linq;
using Hangfire.Mongo.Database;
using MongoDB.Bson;
using MongoDB.Driver;

namespace Hangfire.Mongo.UtcDateTime
{
    /// <summary>
    /// Uses an aggregation pipeline on the Schema collection to retrieve server time.
    /// </summary>
    public sealed class AggregationUtcDateTimeStrategy : UtcDateTimeStrategy
    {
        /// <summary>
        /// Obtain current UTC time using the aggregate pipeline with $$NOW.
        /// </summary>
        /// <param name="dbContext">MongoDB context.</param>
        /// <returns>The UTC time reported by MongoDB.</returns>
        public override DateTime GetUtcDateTime(HangfireDbContext dbContext)
        {
            var pipeline = new[]
            {
                new BsonDocument("$project", new BsonDocument("date", "$$NOW"))
            };

            var time = dbContext.Schema.Aggregate<BsonDocument>(pipeline).FirstOrDefault();
            if (time is null)
            {
                throw new InvalidOperationException("No documents in the schema collection");
            }

            return time["date"].ToUniversalTime();
        }
    }
}
