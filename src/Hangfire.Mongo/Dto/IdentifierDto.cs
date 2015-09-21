using MongoDB.Bson.Serialization.Attributes;

namespace Hangfire.Mongo.Dto
{
#pragma warning disable 1591
    public class IdentifierDto
    {
        [BsonId]
        public string Id { get; set; }
        
        [BsonElement("seq")]
        public long Seq { get; set; }
    }
#pragma warning restore 1591
}
