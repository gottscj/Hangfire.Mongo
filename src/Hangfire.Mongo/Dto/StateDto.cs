using System;
using System.Collections.Generic;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;

namespace Hangfire.Mongo.Dto
{
#pragma warning disable 1591
    public class StateDto
    {
        public StateDto()
        {

        }
        public StateDto(BsonValue doc)
        {
            Name = doc[nameof(Name)].StringOrNull();
            Reason = doc[nameof(Reason)].StringOrNull();
            CreatedAt = doc[nameof(CreatedAt)].ToUniversalTime();
            Data = new Dictionary<string, string>();

            foreach (var b in doc[nameof(Data)].AsBsonDocument)
            {
                Data[b.Name] = b.Value.StringOrNull();
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
                [nameof(Name)] = BsonValue.Create(Name),
                [nameof(Reason)] = BsonValue.Create(Reason),
                [nameof(CreatedAt)] = BsonValue.Create(CreatedAt.ToUniversalTime())
            };
            var data = new BsonDocument();

            if(Data != null)
            {
                foreach (var d in Data)
                {
                    data[d.Key] = BsonValue.Create(d.Value);
                }
            }
            
            document[nameof(Data)] = data;
            return document;
        }
    }
#pragma warning restore 1591
}