using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;

namespace Hangfire.Mongo.Dto
{
#pragma warning disable 1591
    public enum EventType
    {
        JobEnqueued = 0,
        LockReleased = 1
    }
    
    public class EventDto
    {
        [BsonId]
        public ObjectId Id { get; set; }

        [BsonElement(nameof(Type))]
        public EventType Type { get; set; }
        
        [BsonElement(nameof(Value))]
        public string Value { get; set; }
    }
#pragma warning restore 1591
}