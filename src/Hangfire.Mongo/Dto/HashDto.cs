using System.Collections.Generic;

namespace Hangfire.Mongo.Dto
{
#pragma warning disable 1591
    public class HashDto : KeyJobDto
    {
        public Dictionary<string, string> Fields { get; set; }
    }
#pragma warning restore 1591
}