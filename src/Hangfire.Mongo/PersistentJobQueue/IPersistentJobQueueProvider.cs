using Hangfire.Mongo.Database;

namespace Hangfire.Mongo.PersistentJobQueue
{
#pragma warning disable 1591
    public interface IPersistentJobQueueProvider
    {
        IPersistentJobQueue GetJobQueue(HangfireDbContext connection);

        IPersistentJobQueueMonitoringApi GetJobQueueMonitoringApi(HangfireDbContext connection);
    }
#pragma warning restore 1591
}