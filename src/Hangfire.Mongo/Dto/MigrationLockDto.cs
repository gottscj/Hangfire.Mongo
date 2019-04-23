using System;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;

namespace Hangfire.Mongo.Dto
{
#pragma warning disable 1591
    public class MigrationLockDto
    {
        [BsonId]
        public ObjectId Id { get; set; }
        
        [BsonElement(nameof(ExpireAt))]
        [BsonDateTimeOptions(Kind = DateTimeKind.Utc)]
        public DateTime ExpireAt { get; set; }
    }
#pragma warning restore 1591
}