using System;
using System.Collections.Generic;
using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Bson.Serialization.IdGenerators;

namespace Hangfire.Mongo.Dto
{
#pragma warning disable 1591
    public class JobDto
    {
        [BsonId(IdGenerator = typeof(StringObjectIdGenerator))]
        public string Id { get; set; }

        [BsonElement(nameof(StateName))]
        public string StateName { get; set; }

        [BsonElement(nameof(InvocationData))]
        public string InvocationData { get; set; }

        [BsonElement(nameof(Arguments))]
        public string Arguments { get; set; }

        [BsonElement(nameof(Parameters))]
        public Dictionary<string, string> Parameters { get; set; } = new Dictionary<string, string>();

        [BsonElement(nameof(StateHistory))]
        public StateDto[] StateHistory { get; set; } = new StateDto[0];

        [BsonElement(nameof(CreatedAt))]
        public DateTime CreatedAt { get; set; }

        [BsonElement(nameof(ExpireAt))]
        public DateTime? ExpireAt { get; set; }
    }
#pragma warning restore 1591
}