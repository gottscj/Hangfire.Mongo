using System;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;

namespace Hangfire.Mongo.Dto
{
#pragma warning disable 1591
    public class ServerDto
    {
        [BsonId]
        [BsonElement("_id")]
        [BsonRepresentation(BsonType.String)]
        public string Id { get; set; }

        [BsonElement(nameof(WorkerCount))]
        public int WorkerCount { get; set; }

        [BsonElement(nameof(Queues))]
        public string[] Queues { get; set; }

        [BsonElement(nameof(StartedAt))]
        [BsonDateTimeOptions(Kind = DateTimeKind.Utc)]
        public DateTime? StartedAt { get; set; }
        
        [BsonElement(nameof(LastHeartbeat))]
        [BsonDateTimeOptions(Kind = DateTimeKind.Utc)]
        public DateTime? LastHeartbeat { get; set; }
    }
#pragma warning restore 1591
}