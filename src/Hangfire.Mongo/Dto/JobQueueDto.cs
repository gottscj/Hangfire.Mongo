using System;
using MongoDB.Bson;

namespace Hangfire.Mongo.Dto
{

#pragma warning disable 1591
    public class JobQueueDto : BaseJobDto
    {
        public JobQueueDto()
        {

        }
        public JobQueueDto(BsonDocument doc) : base(doc)
        {
            JobId = doc[nameof(JobId)].AsObjectId;
            Queue = doc[nameof(Queue)].StringOrNull();
            FetchedAt = doc[nameof(FetchedAt)].ToNullableUniversalTime();
        }
        public ObjectId JobId { get; set; }

        public string Queue { get; set; }

        public DateTime? FetchedAt { get; set; }

        protected override void Serialize(BsonDocument document)
        {
            document[nameof(JobId)] = JobId;
            document[nameof(Queue)] = Queue;
            document[nameof(FetchedAt)] = BsonValue.Create(FetchedAt?.ToUniversalTime()); 
            document["_t"].AsBsonArray.Add(nameof(JobQueueDto));
        }
    }
#pragma warning restore 1591
}