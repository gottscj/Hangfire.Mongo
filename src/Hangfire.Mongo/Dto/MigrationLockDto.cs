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
            if (doc.TryGetValue(nameof(OwnerToken), out var ownerToken))
            {
                OwnerToken = ownerToken.StringOrNull();
            }
        }
        public ObjectId Id { get; set; }

        public DateTime ExpireAt { get; set; }

        public string OwnerToken { get; set; }

        public BsonDocument Serialize()
        {
            return new BsonDocument
            {
                ["_id"] = Id,
                [nameof(ExpireAt)] = ExpireAt.ToUniversalTime(),
                [nameof(OwnerToken)] = OwnerToken.ToBsonValue(),
            };
        }
    }
#pragma warning restore 1591
}
