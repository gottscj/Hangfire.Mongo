using System.Data;
using Hangfire.Mongo.Database;

namespace Hangfire.Mongo.PersistentJobQueue
{
	public interface IPersistentJobQueueProvider
	{
		IPersistentJobQueue GetJobQueue(HangfireDbContext connection);

		IPersistentJobQueueMonitoringApi GetJobQueueMonitoringApi(HangfireDbContext connection);
	}
}