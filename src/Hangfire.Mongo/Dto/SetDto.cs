using MongoDB.Bson.Serialization.Attributes;

namespace Hangfire.Mongo.Dto
{
#pragma warning disable 1591
    public class SetDto : KeyJobDto
    {
        [BsonElement(nameof(Score))]
        public double Score { get; set; }

        [BsonElement(nameof(Value))]
        public string Value { get; set; }
    }
#pragma warning restore 1591
}