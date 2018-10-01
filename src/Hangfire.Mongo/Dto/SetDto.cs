namespace Hangfire.Mongo.Dto
{
#pragma warning disable 1591
    public class SetDto : KeyJobDto
    {
        public double Score { get; set; }

        public string Value { get; set; }
    }
#pragma warning restore 1591
}