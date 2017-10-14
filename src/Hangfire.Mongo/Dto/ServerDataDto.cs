using System;

namespace Hangfire.Mongo.Dto
{
#pragma warning disable 1591
    internal class ServerDataDto
    {
        public int WorkerCount { get; set; }

        public string[] Queues { get; set; }

        public DateTime? StartedAt { get; set; }
    }
#pragma warning restore 1591
}