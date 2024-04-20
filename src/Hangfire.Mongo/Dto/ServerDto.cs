using System;
using System.Linq;
using MongoDB.Bson;

namespace Hangfire.Mongo.Dto
{
#pragma warning disable 1591
    public class ServerDto
    {
        public ServerDto()
        {

        }
        public ServerDto(BsonDocument doc)
        {
            Id = doc["_id"].AsString;
            WorkerCount = doc[nameof(WorkerCount)].AsInt32;
            if (doc.TryGetValue(nameof(Queues), out var queues))
            {
                Queues = queues.AsBsonArray.Select(q => q.StringOrNull()).ToArray();
            }
            if (doc.TryGetValue(nameof(StartedAt), out var startedAt))
            {
                StartedAt = startedAt.ToNullableUniversalTime();
            }
            if (doc.TryGetValue(nameof(LastHeartbeat), out var lastHeartbeat))
            {
                LastHeartbeat = lastHeartbeat.ToNullableUniversalTime();
            }
        }
        public string Id { get; set; }

        public int WorkerCount { get; set; }

        public string[] Queues { get; set; }

        public DateTime? StartedAt { get; set; }

        public DateTime? LastHeartbeat { get; set; }

        /// <summary>
        /// Serializes to BsonDocument
        /// </summary>
        /// <returns></returns>
        public BsonDocument Serialize()
        {
            return new BsonDocument
            {
                [nameof(WorkerCount)] = WorkerCount,
                [nameof(Queues)] = Queues != null ? new BsonArray(Queues) : new BsonArray(),
                [nameof(StartedAt)] = StartedAt?.ToUniversalTime(),
                [nameof(LastHeartbeat)] = LastHeartbeat?.ToUniversalTime(),
                ["_id"] = Id
            };
        }
#pragma warning restore 1591
    }
}