using System;
using MongoDB.Bson.Serialization.Attributes;

namespace Hangfire.Mongo.Dto
{
#pragma warning disable 1591
    internal class ServerDataDto
    {
        [BsonElement(nameof(WorkerCount))]
        public int WorkerCount { get; set; }

        [BsonElement(nameof(Queues))]
        public string[] Queues { get; set; }

        [BsonElement(nameof(StartedAt))]
        public DateTime? StartedAt { get; set; }
    }
#pragma warning restore 1591
}