using System;
using MongoDB.Bson;

namespace Hangfire.Mongo.Dto
{
#pragma warning disable 1591
    public class JobEnqueuedDto
    {
        public ObjectId Id { get; set; }

        public string Queue { get; set; }
    }
#pragma warning restore 1591
}