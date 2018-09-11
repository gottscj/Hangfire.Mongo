using System;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;

namespace Hangfire.Mongo.Dto
{

#pragma warning disable 1591
    public class JobQueueDto : BaseJobDto
    {
        public ObjectId JobId { get; set; }

        public string Queue { get; set; }

        public DateTime? FetchedAt { get; set; }
    }
#pragma warning restore 1591
}