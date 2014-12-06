using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;

namespace Hangfire.Mongo.Dto
{
	public class JobParameterDto
	{
		[BsonId]
		public ObjectId Id { get; set; }

		public int JobId { get; set; }

		public string Name { get; set; }

		public string Value { get; set; }
	}
}