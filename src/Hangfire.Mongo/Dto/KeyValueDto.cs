using System;

namespace Hangfire.Mongo.Dto
{
#pragma warning disable 1591
    public class KeyValueDto : JobGraphDto
    {
        public string Key { get; set; }

        public object Value { get; set; }
    }

#pragma warning restore 1591
}
