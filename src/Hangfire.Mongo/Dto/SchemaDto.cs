using MongoDB.Bson.Serialization.Attributes;

namespace Hangfire.Mongo.Dto
{
    /// <summary>
    /// Holds the schema version of the database
    /// </summary>
    public class SchemaDto
    {
        /// <summary>
        /// The schema version
        /// </summary>
        [BsonId]
        [BsonElement(nameof(Version))]
        public MongoSchema Version { get; set; }

        /// <summary>
        /// The identifier of the database.
        /// Will be initialized along with the database
        /// and will nerver change.
        /// </summary>
        [BsonIgnoreIfNull]
        [BsonElement(nameof(Identifier))]
        public string Identifier { get; set; }

    }
}