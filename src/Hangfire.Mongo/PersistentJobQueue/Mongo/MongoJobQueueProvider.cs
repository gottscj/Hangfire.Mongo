using System;
using Hangfire.Mongo.Database;

namespace Hangfire.Mongo.PersistentJobQueue.Mongo
{
#pragma warning disable 1591
    internal class MongoJobQueueProvider : IPersistentJobQueueProvider
    {
        private readonly MongoStorageOptions _options;

        public MongoJobQueueProvider(MongoStorageOptions options)
        {
            if (options == null)
                throw new ArgumentNullException("options");

            _options = options;
        }

        public IPersistentJobQueue GetJobQueue(HangfireDbContext connection)
        {
            return new MongoJobQueue(connection, _options);
        }

        public IPersistentJobQueueMonitoringApi GetJobQueueMonitoringApi(HangfireDbContext connection)
        {
            return new MongoJobQueueMonitoringApi(connection);
        }
    }
#pragma warning restore 1591
}