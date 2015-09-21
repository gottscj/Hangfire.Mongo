using MongoDB.Bson.Serialization.Attributes;

namespace Hangfire.Mongo.Dto
{
#pragma warning disable 1591
    public class SchemaDto
    {
        [BsonId]
        public int Version { get; set; }
    }
#pragma warning restore 1591
}