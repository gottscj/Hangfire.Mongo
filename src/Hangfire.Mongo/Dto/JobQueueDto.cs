using System;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;

namespace Hangfire.Mongo.Dto
{

#pragma warning disable 1591
    public class JobQueueDto : BaseJobDto
    {
        [BsonElement(nameof(JobId))]
        public ObjectId JobId { get; set; }

        [BsonElement(nameof(Queue))]
        public string Queue { get; set; }

        [BsonElement(nameof(FetchedAt))]
        [BsonDateTimeOptions(Kind = DateTimeKind.Utc)]
        public DateTime? FetchedAt { get; set; }
    }
#pragma warning restore 1591
}