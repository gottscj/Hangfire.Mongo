namespace Hangfire.Mongo.PersistentJobQueue
{
	public class EnqueuedAndFetchedCountDto
	{
		public int? EnqueuedCount { get; set; }

		public int? FetchedCount { get; set; }
	}
}