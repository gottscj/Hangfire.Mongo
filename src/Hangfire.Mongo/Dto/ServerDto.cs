using System;
using MongoDB.Bson.Serialization.Attributes;

namespace Hangfire.Mongo.Dto
{
#pragma warning disable 1591
    public class ServerDto
    {
        [BsonId]
        [BsonElement("_id")]
        public string Id { get; set; }

        [BsonElement(nameof(Data))]
        public string Data { get; set; }

        [BsonElement(nameof(LastHeartbeat))]
        public DateTime? LastHeartbeat { get; set; }
    }
#pragma warning restore 1591
}