using System;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;

namespace Hangfire.Mongo.Dto
{
#pragma warning disable 1591
    [BsonDiscriminator(RootClass = true)]
    [BsonKnownTypes(
        typeof(ExpiringKeyValueDto),
        typeof(AggregatedCounterDto),
        typeof(CounterDto),
        typeof(ListDto),
        typeof(SetDto),
        typeof(HashDto))]
    public class KeyValueDto
    {
        [BsonId]
        public ObjectId Id { get; set; }

        [BsonElement(nameof(Key))]
        public string Key { get; set; }

        [BsonElement(nameof(Value))]
        public object Value { get; set; }
    }

    public class ExpiringKeyValueDto : KeyValueDto
    {
        [BsonElement(nameof(ExpireAt))]
        public DateTime? ExpireAt { get; set; }
    }
#pragma warning restore 1591
}
