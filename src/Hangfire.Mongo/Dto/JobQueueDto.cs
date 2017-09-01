using System;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;

namespace Hangfire.Mongo.Dto
{

#pragma warning disable 1591
    public class JobQueueDto
    {
        [BsonId]
        public ObjectId Id { get; set; }

        [BsonElement(nameof(JobId))]
        public string JobId { get; set; }

        [BsonElement(nameof(Queue))]
        public string Queue { get; set; }

        [BsonElement(nameof(FetchedAt))]
        public DateTime? FetchedAt { get; set; }

    }
#pragma warning restore 1591
}