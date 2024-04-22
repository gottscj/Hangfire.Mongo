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
            if (doc.TryGetValue(nameof(Item), out var item))
            {
                Item = item.StringOrNull();
            }
            if (doc.TryGetValue(nameof(Value), out var value))
            {
                Value = value.StringOrNull();
            }
        }
        public string Item { get; set; }

        public string Value { get; set; }
        protected override void Serialize(BsonDocument document)
        {
            base.Serialize(document);
            document[nameof(Item)] = Item.ToBsonValue();
            document[nameof(Value)] = Value.ToBsonValue();
            document["_t"].AsBsonArray.Add(nameof(ListDto));
        }
    }
#pragma warning restore 1591
}