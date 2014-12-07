using System;

namespace Hangfire.Mongo.Dto
{
	internal class ServerDataDto
	{
		public int WorkerCount { get; set; }

		public string[] Queues { get; set; }

		public DateTime? StartedAt { get; set; }
	}
}