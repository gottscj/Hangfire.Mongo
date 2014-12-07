using System;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;

namespace Hangfire.Mongo.Dto
{
	public class SetDto
	{
		[BsonId]
		public ObjectId Id { get; set; }

		public string Key { get; set; }

		public double Score { get; set; }

		public string Value { get; set; }

		public DateTime ExpireAt { get; set; }
	}
}