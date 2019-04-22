using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;

namespace Hangfire.Mongo.Dto
{
#pragma warning disable 1591
    public enum NotificationType
    {
        JobEnqueued = 0,
        LockReleased = 1
    }
    
    public class NotificationDto
    {
        public static NotificationDto JobEnqueued(string queue)
        {
            return new NotificationDto
            {
                Id = ObjectId.GenerateNewId(),
                Type = NotificationType.JobEnqueued,
                Value = queue
            };
        }

        public static NotificationDto LockReleased(string resource)
        {
            return new NotificationDto
            {
                Id = ObjectId.GenerateNewId(),
                Type = NotificationType.LockReleased,
                Value = resource
            };
        }
        [BsonId]
        public ObjectId Id { get; set; }

        [BsonElement(nameof(Type))]
        public NotificationType Type { get; set; }
        
        [BsonElement(nameof(Value))]
        public string Value { get; set; }
    }
#pragma warning restore 1591
}