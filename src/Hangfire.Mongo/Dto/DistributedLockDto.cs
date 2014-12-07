using System;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;

namespace Hangfire.Mongo.Dto
{
	public class DistributedLockDto
	{
		[BsonId]
		public ObjectId Id { get; set; }

		public string Resource { get; set; }

		public string ClientId { get; set; }

		public int LockCount { get; set; }

		public DateTime Heartbeat { get; set; }
	}
}