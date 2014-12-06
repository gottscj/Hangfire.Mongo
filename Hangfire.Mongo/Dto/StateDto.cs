using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using System;

namespace Hangfire.Mongo.Dto
{
	public class StateDto
	{
		[BsonId]
		public ObjectId Id { get; set; }

		public int JobId { get; set; }

		public string Name { get; set; }

		public string Reason { get; set; }

		public DateTime CreatedAt { get; set; }

		public string Data { get; set; }
	}
}