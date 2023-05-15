using System;
using System.Collections.Generic;
using MongoDB.Bson;

namespace Hangfire.Mongo.Dto
{
#pragma warning disable 1591
    public class StateDto
    {
        public StateDto()
        {

        }
        public StateDto(BsonDocument doc)
        {
            Name = doc[nameof(Name)].StringOrNull();
            Reason = doc[nameof(Reason)].StringOrNull();
            CreatedAt = doc[nameof(CreatedAt)].ToUniversalTime();
            Data = new Dictionary<string, string>();
            if(doc.TryGetValue(nameof(Data), out var data))
            {
                foreach (var b in data.AsBsonDocument)
                {
                    Data[b.Name] = b.Value.StringOrNull();
                }
            }
            
        }
        public string Name { get; set; }

        public string Reason { get; set; }

        public DateTime CreatedAt { get; set; }

        public Dictionary<string, string> Data { get; set; }

        public BsonDocument Serialize()
        {
            var document = new BsonDocument
            {
                [nameof(Name)] = Name.ToBsonValue(),
                [nameof(Reason)] = Reason.ToBsonValue(),
                [nameof(CreatedAt)] = CreatedAt.ToUniversalTime()
            };
            var data = new BsonDocument();

            if(Data != null)
            {
                foreach (var d in Data)
                {
                    data[d.Key] = d.Value.ToBsonValue();
                }
            }
            
            document[nameof(Data)] = data;
            return document;
        }
    }
#pragma warning restore 1591
}