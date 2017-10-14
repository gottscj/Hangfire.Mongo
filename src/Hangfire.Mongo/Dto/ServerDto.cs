using System;
using MongoDB.Bson.Serialization.Attributes;

namespace Hangfire.Mongo.Dto
{
#pragma warning disable 1591
    public class ServerDto
    {
        [BsonId]
        public string Id { get; set; }

        public string Data { get; set; }

        public DateTime? LastHeartbeat { get; set; }
    }
#pragma warning restore 1591
}