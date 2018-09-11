using System;

namespace Hangfire.Mongo.Dto
{
#pragma warning disable 1591
    
    public abstract class ExpiringJobDto : BaseJobDto
    {
        public DateTime? ExpireAt { get; set; }
    }
    
#pragma warning restore 1591
}