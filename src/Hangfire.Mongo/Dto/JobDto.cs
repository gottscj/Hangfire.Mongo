using System;
using System.Collections.Generic;
using MongoDB.Bson;

namespace Hangfire.Mongo.Dto
{
#pragma warning disable 1591
    public class JobDto : ExpiringJobDto
    {
        public JobDto()
        {

        }
        public JobDto(BsonDocument doc) : base(doc)
        {
            if (doc == null)
            {
                return;
            }

            if (doc.TryGetValue(nameof(Queue), out var queue))
            {
                Queue = queue.StringOrNull();
            }
            if (doc.TryGetValue(nameof(FetchedAt), out var fetchedAt))
            {
                FetchedAt = fetchedAt.ToNullableLocalTime();
            }
            if (doc.TryGetValue(nameof(StateName), out var stateName))
            {
                StateName = stateName.StringOrNull();
            }
            if (doc.TryGetValue(nameof(InvocationData), out var invocationData))
            {
                InvocationData = invocationData.StringOrNull();
            }
            if (doc.TryGetValue(nameof(Arguments), out var arguments))
            {
                Arguments = arguments.StringOrNull();
            }
            Parameters = new Dictionary<string, string>();
            if (doc.TryGetValue(nameof(Parameters), out var parameters))
            {
                foreach (var b in parameters.AsBsonDocument)
                {
                    Parameters[b.Name] = b.Value.StringOrNull();
                }

            }

            CreatedAt = doc[nameof(CreatedAt)].ToUniversalTime();
        }

        public string StateName { get; set; }

        public string InvocationData { get; set; }

        public string Arguments { get; set; }

        public Dictionary<string, string> Parameters { get; set; } = new Dictionary<string, string>();
        
        public DateTime CreatedAt { get; set; }

        public DateTime? FetchedAt { get; set; }

        public string Queue { get; set; }

        protected override void Serialize(BsonDocument doc)
        {
            base.Serialize(doc);
            doc[nameof(Queue)] = Queue.ToBsonValue();
            doc[nameof(FetchedAt)] = FetchedAt;
            doc[nameof(StateName)] = StateName.ToBsonValue();
            doc[nameof(InvocationData)] = InvocationData.ToBsonValue();
            doc[nameof(Arguments)] = Arguments.ToBsonValue();
            var parameters = new BsonDocument();
            foreach (var p in Parameters)
            {
                parameters[p.Key] = p.Value.ToBsonValue();
            }
            doc[nameof(Parameters)] = parameters;
            doc[nameof(CreatedAt)] = CreatedAt.ToUniversalTime();
            doc["_t"].AsBsonArray.Add(nameof(JobDto));
        }
    }
#pragma warning restore 1591
}