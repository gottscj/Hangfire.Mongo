using System.Collections.Generic;
using MongoDB.Bson;

namespace Hangfire.Mongo.Dto
{
#pragma warning disable 1591
    public class HashDto : KeyJobDto
    {
        public HashDto()
        {

        }
        public HashDto(BsonDocument doc) : base(doc)
        {
            if (doc == null)
            {
                return;
            }
            Fields = new Dictionary<string, string>();
            if(doc.TryGetValue(nameof(Fields), out var fields) && fields != BsonNull.Value)
            {
                foreach (var b in fields.AsBsonDocument)
                {
                    Fields[b.Name] = b.Value.StringOrNull();
                }
            }
            
        }

        public Dictionary<string, string> Fields { get; set; }

        protected override void Serialize(BsonDocument document)
        {
            base.Serialize(document);
            document[nameof(Fields)] = BsonNull.Value;

            if (Fields != null)
            {
                var fields = new BsonDocument();
                foreach (var field in Fields)
                {
                    fields[field.Key] = field.Value.ToBsonValue();
                }
                document[nameof(Fields)] = fields;
            }
            
            document["_t"].AsBsonArray.Add(nameof(HashDto));
        }
    }
#pragma warning restore 1591
}