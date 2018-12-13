using System;
using System.Collections.Generic;
using MongoDB.Bson.Serialization.Attributes;

namespace Hangfire.Mongo.Dto
{
#pragma warning disable 1591
    public class JobDto : ExpiringJobDto
    {
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
        [BsonDateTimeOptions(Kind = DateTimeKind.Utc)]
        public DateTime CreatedAt { get; set; }
    }
#pragma warning restore 1591
}