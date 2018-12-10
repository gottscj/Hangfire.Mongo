using System;
using MongoDB.Bson.Serialization.Attributes;

namespace Hangfire.Mongo.Dto
{
#pragma warning disable 1591
    internal class ServerDataDto
    {
        public int WorkerCount { get; set; }

        public string[] Queues { get; set; }

        [BsonDateTimeOptions(Kind = DateTimeKind.Utc)]
        public DateTime? StartedAt { get; set; }
    }
#pragma warning restore 1591
}