using System;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;

namespace Hangfire.Mongo.Dto
{
#pragma warning disable 1591
    public class SignalDto
    {
        [BsonId]
        public ObjectId Id { get; set; }

        public bool Signaled { get; set; }

        public string Name { get; set; }

        public DateTime TimeStamp { get; set; }
    }
#pragma warning restore 1591
}