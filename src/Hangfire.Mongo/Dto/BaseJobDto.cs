using System;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;

namespace Hangfire.Mongo.Dto
{
#pragma warning disable 1591
    [BsonDiscriminator(RootClass = true)]
    [BsonKnownTypes(
        typeof(AggregatedCounterDto),
        typeof(CounterDto),
        typeof(ListDto),
        typeof(SetDto),
        typeof(HashDto),
        typeof(JobDto),
        typeof(KeyJobDto))]
    public class BaseJobDto
    {
        [BsonId]
        public ObjectId Id { get; set; }
    }
#pragma warning restore 1591
}