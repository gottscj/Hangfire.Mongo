using Hangfire.Mongo.Database;

namespace Hangfire.Mongo.PersistentJobQueue
{
#pragma warning disable 1591
    public interface IPersistentJobQueueProvider
    {
        IPersistentJobQueue GetJobQueue(HangfireDbContext database);

        IPersistentJobQueueMonitoringApi GetJobQueueMonitoringApi(HangfireDbContext database);
    }
#pragma warning restore 1591
}