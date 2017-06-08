using System.Collections.Generic;

namespace Hangfire.Mongo.PersistentJobQueue
{
#pragma warning disable 1591
    public interface IPersistentJobQueueMonitoringApi
    {
        IEnumerable<string> GetQueues();

        IEnumerable<string> GetEnqueuedJobIds(string queue, int from, int perPage);

        IEnumerable<string> GetFetchedJobIds(string queue, int from, int perPage);

        EnqueuedAndFetchedCountDto GetEnqueuedAndFetchedCount(string queue);
    }
#pragma warning restore 1591
}
