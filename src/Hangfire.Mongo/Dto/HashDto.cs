using System.Collections.Generic;
using MongoDB.Bson.Serialization.Attributes;

namespace Hangfire.Mongo.Dto
{
#pragma warning disable 1591
    public class HashDto : KeyJobDto
    {
        [BsonElement(nameof(Fields))]
        public Dictionary<string, string> Fields { get; set; }
    }
#pragma warning restore 1591
}