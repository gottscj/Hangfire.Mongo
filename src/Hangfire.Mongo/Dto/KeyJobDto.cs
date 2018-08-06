using System;

namespace Hangfire.Mongo.Dto
{
#pragma warning disable 1591
    public abstract class KeyJobDto : BaseJobDto
    {
        public string Key { get; set; }
    }

#pragma warning restore 1591
}
