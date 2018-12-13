using System;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;

namespace Hangfire.Mongo.Dto
{
    /// <summary>
    /// Document used for holding a distributed lock in mongo.
    /// </summary>
    public class DistributedLockDto
    {
        /// <summary>
        /// The unique id of the document.
        /// </summary>
        [BsonId]
        [BsonElement("_id")]
        public ObjectId Id { get; set; }

        /// <summary>
        /// The name of the resource being held.
        /// </summary>
        [BsonElement(nameof(Resource))]
        public string Resource { get; set; }

        /// <summary>
        /// The timestamp for when the lock expires.
        /// This is used if the lock is not maintained or 
        /// cleaned up by the owner (e.g. process was shut down).
        /// </summary>
        [BsonElement(nameof(ExpireAt))]
        [BsonDateTimeOptions(Kind = DateTimeKind.Utc)]
        public DateTime ExpireAt { get; set; }
    }
}