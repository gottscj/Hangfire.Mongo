using MongoDB.Bson;

namespace Hangfire.Mongo.Dto
{
#pragma warning disable 1591
    public class CounterDto : KeyJobDto
    {
        public CounterDto()
        {

        }
        public CounterDto(BsonDocument doc) : base(doc)
        {
            if(doc == null)
            {
                return;
            }
            Value = doc[nameof(Value)].AsInt64;
        }

        public long Value { get; set; }

        protected override void Serialize(BsonDocument document)
        {
            base.Serialize(document);
            document[nameof(Value)] = Value;
            document["_t"].AsBsonArray.Add(nameof(CounterDto));
        }
    }
#pragma warning restore 1591
}