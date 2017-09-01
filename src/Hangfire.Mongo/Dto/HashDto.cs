using MongoDB.Bson.Serialization.Attributes;

namespace Hangfire.Mongo.Dto
{
#pragma warning disable 1591
    public class HashDto : ExpiringKeyValueDto
    {
        [BsonElement(nameof(Field))]
        public string Field { get; set; }
    }
#pragma warning restore 1591
}