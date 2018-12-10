using System;
using System.Collections.Generic;
using MongoDB.Bson.Serialization.Attributes;

namespace Hangfire.Mongo.Dto
{
#pragma warning disable 1591
    public class StateDto
    {
        public string Name { get; set; }

        public string Reason { get; set; }

        [BsonDateTimeOptions(Kind = DateTimeKind.Utc)]
        public DateTime CreatedAt { get; set; }

        public Dictionary<string, string> Data { get; set; }
    }
#pragma warning restore 1591
}