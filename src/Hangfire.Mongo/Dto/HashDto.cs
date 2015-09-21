using System;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;

namespace Hangfire.Mongo.Dto
{
#pragma warning disable 1591
    public class HashDto
    {
        [BsonId]
        public ObjectId Id { get; set; }

        public string Key { get; set; }

        public string Field { get; set; }

        public string Value { get; set; }

        public DateTime? ExpireAt { get; set; }
    }
#pragma warning restore 1591
}