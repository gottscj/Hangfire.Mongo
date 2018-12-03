using System;
using System.Collections.Generic;

namespace Hangfire.Mongo
{
#pragma warning disable 1591
    public class JobSummary
    {
        public string Id { get; set; }

        public string InvocationData { get; set; }

        public string Arguments { get; set; }

        public DateTime CreatedAt { get; set; }

        public DateTime? ExpireAt { get; set; }

        public DateTime? FetchedAt { get; set; }

        public string StateName { get; set; }

        public string StateReason { get; set; }

        public Dictionary<string, string> StateData { get; set; }
    }
#pragma warning restore 1591
}