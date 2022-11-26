using MongoDB.Bson;

namespace Hangfire.Mongo.Dto
{
#pragma warning disable 1591
    public class ListDto : ExpiringJobDto
    {
        public ListDto()
        {

        }
        public ListDto(BsonDocument doc) : base(doc)
        {
            Item = doc[nameof(Item)].StringOrNull();
            Value = doc[nameof(Value)].StringOrNull();
        }
        public string Item { get; set; }

        public string Value { get; set; }
        protected override void Serialize(BsonDocument document)
        {
            base.Serialize(document);
            document[nameof(Item)] = BsonValue.Create(Item);
            document[nameof(Value)] = BsonValue.Create(Value);
            document["_t"].AsBsonArray.Add(nameof(ListDto));
        }
    }
#pragma warning restore 1591
}