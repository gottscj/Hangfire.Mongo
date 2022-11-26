using MongoDB.Bson;

namespace Hangfire.Mongo.Dto
{
#pragma warning disable 1591
    public abstract class BaseJobDto
    {
        public ObjectId Id { get; set; }

        public BaseJobDto()
        {
            Id = ObjectId.GenerateNewId();
        }

        protected BaseJobDto(BsonDocument doc)
        {
            if (doc == null)
            {
                return;
            }
            Id = doc["_id"].AsObjectId;
        }

        public BsonDocument Serialize()
        {
            var document = new BsonDocument
            {
                { "_id", Id },
                { "_t", new BsonArray { nameof(BaseJobDto) }}
            };
            Serialize(document);
            return document;
        }
        
        protected abstract void Serialize(BsonDocument document);        
    }
#pragma warning restore 1591
}