using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using System;	

namespace Hangfire.Mongo.Dto
{
	public class ListDto
	{
		[BsonId]
		public ObjectId Id { get; set; }

		public string Key { get; set; }

		public string Value { get; set; }

		public DateTime ExpireAt { get; set; }
	}
}