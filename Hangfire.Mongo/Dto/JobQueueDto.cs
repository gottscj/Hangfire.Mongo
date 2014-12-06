using Hangfire.Mongo.MongoUtils;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using System;

namespace Hangfire.Mongo.Dto
{
	public class JobQueueDto
	{
		[BsonId(IdGenerator = typeof(AutoIncrementIdGenerator))]
		public int Id { get; set; }

		public int JobId { get; set; }

		public string Queue { get; set; }

		public DateTime? FetchedAt { get; set; }
	}
}