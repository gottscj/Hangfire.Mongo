using MongoDB.Bson.Serialization.Attributes;
using System;

namespace Hangfire.Mongo.Dto
{
	public class ServerDto
	{
		[BsonId]
		public string Id { get; set; }

		public string Data { get; set; }

		public DateTime LastHeartbeat { get; set; }
	}
}