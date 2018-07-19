using MongoDB.Bson.Serialization.Attributes;

namespace Hangfire.Mongo.Dto
{
#pragma warning disable 1591
    public class MigrationLockDto
    {
        [BsonId]
        public string Lock { get; set; }
    }
#pragma warning restore 1591
}