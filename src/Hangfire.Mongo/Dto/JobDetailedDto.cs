using System;
using System.Collections.Generic;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;

namespace Hangfire.Mongo.Dto
{
#pragma warning disable 1591
    public class JobDetailedDto
    {
        [BsonId]
        public string Id { get; set; }

        [BsonElement(nameof(InvocationData))]
        public string InvocationData { get; set; }

        [BsonElement(nameof(Arguments))]
        public string Arguments { get; set; }

        [BsonElement(nameof(CreatedAt))]
        public DateTime CreatedAt { get; set; }

        [BsonElement(nameof(ExpireAt))]
        public DateTime? ExpireAt { get; set; }

        [BsonElement(nameof(FetchedAt))]
        public DateTime? FetchedAt { get; set; }

        [BsonElement(nameof(StateId))]
        public ObjectId StateId { get; set; }

        [BsonElement(nameof(StateName))]
        public string StateName { get; set; }

        [BsonElement(nameof(StateReason))]
        public string StateReason { get; set; }

        [BsonElement(nameof(StateData))]
        public Dictionary<string, string> StateData { get; set; }
    }
#pragma warning restore 1591
}