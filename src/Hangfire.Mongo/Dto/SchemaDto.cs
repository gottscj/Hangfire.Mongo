using System;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;

namespace Hangfire.Mongo.Dto
{
    /// <summary>
    /// Holds the schema version of the database
    /// </summary>
    public class SchemaDto
    {
        /// <summary>
        /// ctor
        /// </summary>
        public SchemaDto()
        {

        }
        /// <summary>
        /// fills properties from bson doc
        /// </summary>
        /// <param name="doc"></param>
        public SchemaDto(BsonDocument doc)
        {
            if(doc == null)
            {
                return;
            }

            var id = doc["_id"];
            if (id.IsString)
            {
                // Before schema version 20, the _id was stored as a string
                Version = (MongoSchema)Enum.Parse(typeof(MongoSchema), id.AsString);
            }
            else
            {
                Version = (MongoSchema)id.AsInt32;
            }
        }

        /// <summary>
        /// The schema version
        /// </summary>
        [BsonElement("_id")]
        public MongoSchema Version { get; set; }

        /// <summary>
        /// Serializes to BsonDocument
        /// </summary>
        /// <returns></returns>
        public BsonDocument Serialize()
        {
            return new BsonDocument
            {
                ["_id"] = (int)Version
            };
        }
    }
}