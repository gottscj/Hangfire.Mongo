using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;

namespace Hangfire.Mongo.Dto
{
#pragma warning disable 1591
    public class JobEnqueuedDto
    {
        public ObjectId Id { get; set; }

        [BsonElement("Queue")]
        public string Queue { get; set; }
    }
#pragma warning restore 1591
}