using System;
using MongoDB.Bson;

namespace Hangfire.Mongo.Dto
{
    /// <summary>
    /// Document used for holding a distributed lock in mongo.
    /// </summary>
    public class DistributedLockDto
    {
        /// <summary>
        /// ctor
        /// </summary>
        public DistributedLockDto()
        {

        }
        /// <summary>
        /// fills properties from given doc
        /// </summary>
        /// <param name="doc"></param>
        public DistributedLockDto(BsonDocument doc)
        {
            Id = doc["_id"].AsObjectId;
            if (doc.TryGetValue(nameof(Resource), out var resource))
            {
                Resource = resource.StringOrNull();
            }
            ExpireAt = doc[nameof(ExpireAt)].ToUniversalTime();
        }
        /// <summary>
        /// Id
        /// </summary>
        public ObjectId Id { get; set; }

        /// <summary>
        /// The name of the resource being held.
        /// </summary>
        public string Resource { get; set; }

        /// <summary>
        /// The timestamp for when the lock expires.
        /// This is used if the lock is not maintained or 
        /// cleaned up by the owner (e.g. process was shut down).
        /// </summary>
        public DateTime ExpireAt { get; set; }

        /// <summary>
        /// Serializes to BsonDocument
        /// </summary>
        /// <returns></returns>
        public virtual BsonDocument Serialize()
        {
            return new BsonDocument
            {
                ["_id"] = Id,
                [nameof(Resource)] = Resource.ToBsonValue(),
                [nameof(ExpireAt)] = ExpireAt.ToUniversalTime(),
            };
        }
        
    }
}