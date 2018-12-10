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
        public ObjectId Id { get; set; }

        public string InvocationData { get; set; }

        public string Arguments { get; set; }

        [BsonDateTimeOptions(Kind = DateTimeKind.Utc)]
        public DateTime CreatedAt { get; set; }

        [BsonDateTimeOptions(Kind = DateTimeKind.Utc)]
        public DateTime? ExpireAt { get; set; }

        [BsonDateTimeOptions(Kind = DateTimeKind.Utc)]
        public DateTime? FetchedAt { get; set; }

        public ObjectId StateId { get; set; }

        public string StateName { get; set; }

        public string StateReason { get; set; }

        public Dictionary<string, string> StateData { get; set; }
    }
#pragma warning restore 1591
}