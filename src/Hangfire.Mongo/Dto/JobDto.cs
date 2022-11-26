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
            if(doc == null)
            {
                return;
            }

            StateName = doc[nameof(StateName)].StringOrNull();
            InvocationData = doc[nameof(InvocationData)].StringOrNull();
            Arguments = doc[nameof(Arguments)].StringOrNull();
            Parameters = new Dictionary<string, string>();
            foreach (var b in doc[nameof(Parameters)].AsBsonDocument)
            {
                Parameters[b.Name] = b.Value.StringOrNull();
            }
            CreatedAt = doc[nameof(CreatedAt)].ToUniversalTime();
            StateHistory = doc[nameof(StateHistory)].AsBsonArray.Select(b => new StateDto(b)).ToArray();
        }

        public string StateName { get; set; }

        public string InvocationData { get; set; }

        public string Arguments { get; set; }

        public Dictionary<string, string> Parameters { get; set; } = new Dictionary<string, string>();

        public StateDto[] StateHistory { get; set; } = new StateDto[0];

        public DateTime CreatedAt { get; set; }

        protected override void Serialize(BsonDocument document)
        {
            base.Serialize(document);
            document[nameof(StateName)] = BsonValue.Create(StateName);
            document[nameof(InvocationData)] = BsonValue.Create(InvocationData);
            document[nameof(Arguments)] = BsonValue.Create(Arguments);
            var parameters = new BsonDocument();
            foreach (var p in Parameters)
            {
                parameters[p.Key] = BsonValue.Create(p.Value);
            }
            document[nameof(Parameters)] = parameters;
            var history = new BsonArray();
            foreach (var h in StateHistory)
            {
                history.Add(h.Serialize());
            }
            document[nameof(StateHistory)] = history;
            document[nameof(CreatedAt)] = CreatedAt.ToUniversalTime();
            document["_t"].AsBsonArray.Add(nameof(JobDto));
        }
    }
#pragma warning restore 1591
}