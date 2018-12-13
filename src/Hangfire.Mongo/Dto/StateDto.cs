using System;
using System.Collections.Generic;
using MongoDB.Bson.Serialization.Attributes;

namespace Hangfire.Mongo.Dto
{
#pragma warning disable 1591
    public class StateDto
    {
        [BsonElement(nameof(Name))]
        public string Name { get; set; }

        [BsonElement(nameof(Reason))]
        public string Reason { get; set; }

        [BsonElement(nameof(CreatedAt))]
        [BsonDateTimeOptions(Kind = DateTimeKind.Utc)]
        public DateTime CreatedAt { get; set; }

        [BsonElement(nameof(Data))]
        public Dictionary<string, string> Data { get; set; }
    }
#pragma warning restore 1591
}