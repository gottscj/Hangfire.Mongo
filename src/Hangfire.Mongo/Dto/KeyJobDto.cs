using MongoDB.Bson;

namespace Hangfire.Mongo.Dto
{
#pragma warning disable 1591
    public abstract class KeyJobDto : ExpiringJobDto
    {
        public string Key { get; set; }

        protected KeyJobDto()
        {

        }

        protected KeyJobDto(BsonDocument doc) : base(doc)
        {
            if (doc == null)
            {
                return;
            }
            if (doc.TryGetValue(nameof(Key), out var key))
            {
                Key = key.StringOrNull();
            }
        }

        protected override void Serialize(BsonDocument document)
        {
            base.Serialize(document);
            document[nameof(Key)] = Key.ToBsonValue();
            document["_t"].AsBsonArray.Add(nameof(KeyJobDto));
        }
    }

#pragma warning restore 1591
}
