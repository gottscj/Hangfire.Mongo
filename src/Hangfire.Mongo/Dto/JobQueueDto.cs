using System;
using Hangfire.Mongo.MongoUtils;
using MongoDB.Bson.Serialization.Attributes;

namespace Hangfire.Mongo.Dto
{
#pragma warning disable 1591
    public class JobQueueDto
    {
        [BsonId(IdGenerator = typeof(AutoIncrementIntIdGenerator))]
        public int Id { get; set; }

        public int JobId { get; set; }

        public string Queue { get; set; }

        public DateTime? FetchedAt { get; set; }
    }
#pragma warning restore 1591
}