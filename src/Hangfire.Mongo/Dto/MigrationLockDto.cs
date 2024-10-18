using System;
using MongoDB.Bson;

namespace Hangfire.Mongo.Dto
{
#pragma warning disable 1591
    public class MigrationLockDto
    {
        public MigrationLockDto()
        {

        }
        public MigrationLockDto(BsonDocument doc)
        {
            Id = doc["_id"].AsObjectId;
            ExpireAt = doc[nameof(ExpireAt)].ToUniversalTime();
        }
        public ObjectId Id { get; set; }
        
        public DateTime ExpireAt { get; set; }

        public BsonDocument Serialize()
        {
            return new BsonDocument
            {
                ["_id"] = Id,
                [nameof(ExpireAt)] = ExpireAt.ToUniversalTime(),
            };
        }
    }
#pragma warning restore 1591
}