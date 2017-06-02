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

        public string Key { get; set; }

        public object Value { get; set; }
    }

    public class ExpiringKeyValueDto : KeyValueDto
    {
        public DateTime? ExpireAt { get; set; }
    }
#pragma warning restore 1591
}
