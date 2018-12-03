using MongoDB.Bson.Serialization.Attributes;

namespace Hangfire.Mongo.Dto
{
#pragma warning disable 1591
    public class ListDto : KeyJobDto
    {
        [BsonElement(nameof(Value))]
        public string Value { get; set; }
    }
#pragma warning restore 1591
}