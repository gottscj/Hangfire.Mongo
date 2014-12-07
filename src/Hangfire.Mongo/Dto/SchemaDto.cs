using MongoDB.Bson.Serialization.Attributes;

namespace Hangfire.Mongo.Dto
{
	public class SchemaDto
	{
		[BsonId]
		public int Version { get; set; }
	}
}