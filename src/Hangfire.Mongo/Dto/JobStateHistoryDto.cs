using MongoDB.Bson;

namespace Hangfire.Mongo.Dto
{
#pragma warning disable 1591
    public class JobStateHistoryDto : BaseJobDto
    {
        public JobStateHistoryDto()
        {
        }

        public JobStateHistoryDto(BsonDocument doc) : base(doc)
        {
            if (doc == null)
            {
                return;
            }

            if (doc.TryGetValue(nameof(JobId), out var jobId))
            {
                JobId = jobId.AsObjectId;
            }
            if (doc.TryGetValue(nameof(State), out var state))
            {
                State = new StateDto(state.AsBsonDocument);
            }
        }
        public ObjectId JobId { get; set; }
        public StateDto State { get; set; }
        protected override void Serialize(BsonDocument doc)
        {
            BsonValue state = BsonNull.Value;
            if(State != null)
            {
                state = State.Serialize();
            }
            doc[nameof(JobId)] = JobId;
            doc[nameof(State)] = state;
            doc["_t"].AsBsonArray.Add(nameof(JobStateHistoryDto));
        }
    }
#pragma warning restore 1591
}
