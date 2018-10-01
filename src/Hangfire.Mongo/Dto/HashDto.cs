namespace Hangfire.Mongo.Dto
{
#pragma warning disable 1591
    public class HashDto : KeyJobDto
    {
        public string Field { get; set; }

        public string Value { get; set; }
    }
#pragma warning restore 1591
}