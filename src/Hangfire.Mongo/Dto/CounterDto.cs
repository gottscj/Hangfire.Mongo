using MongoDB.Bson.Serialization.Attributes;

namespace Hangfire.Mongo.Dto
{
#pragma warning disable 1591
    [BsonDiscriminator(nameof(CounterDto))]
    public class CounterDto : KeyJobDto
    {
        [BsonElement(nameof(Value))]
        public long Value { get; set; }
    }
#pragma warning restore 1591
}