using System;
using System.Collections.Generic;
using System.Linq;
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

            Queue = doc[nameof(Queue)].StringOrNull();
            FetchedAt = doc[nameof(FetchedAt)].ToNullableLocalTime();
            StateName = doc[nameof(StateName)].StringOrNull();
            InvocationData = doc[nameof(InvocationData)].StringOrNull();
            Arguments = doc[nameof(Arguments)].StringOrNull();
            Parameters = new Dictionary<string, string>();
            if (doc.TryGetValue(nameof(Parameters), out var parameters))
            {
                foreach (var b in parameters.AsBsonDocument)
                {
                    Parameters[b.Name] = b.Value.StringOrNull();
                }

            }

            CreatedAt = doc[nameof(CreatedAt)].ToUniversalTime();
            StateHistory = doc[nameof(StateHistory)]
                .AsBsonArray
                .Select(b => b.AsBsonDocument)
                .Select(b => new StateDto(b))
                .ToArray();
           
            if(doc.TryGetValue(nameof(StateChanged), out var value) && value != BsonNull.Value)
            {
                StateChanged = value.ToNullableUniversalTime();
            }
            else
            {
                StateChanged = CreatedAt;
            }
        }

        public string StateName { get; set; }

        public string InvocationData { get; set; }

        public string Arguments { get; set; }

        public Dictionary<string, string> Parameters { get; set; } = new Dictionary<string, string>();

        public StateDto[] StateHistory { get; set; } = new StateDto[0];

        public DateTime CreatedAt { get; set; }

        public DateTime? FetchedAt { get; set; }

        public DateTime? StateChanged { get; set; }

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
            var history = new BsonArray();
            foreach (var h in StateHistory)
            {
                history.Add(h.Serialize());
            }
            doc[nameof(StateHistory)] = history;
            doc[nameof(CreatedAt)] = CreatedAt.ToUniversalTime();
            doc[nameof(StateChanged)] = StateChanged;
            doc["_t"].AsBsonArray.Add(nameof(JobDto));
        }
    }
#pragma warning restore 1591
}