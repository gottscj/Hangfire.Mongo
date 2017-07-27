using System;
using Hangfire.Mongo.Database;

namespace Hangfire.Mongo.PersistentJobQueue.Mongo
{
#pragma warning disable 1591
    internal class MongoJobQueueProvider : IPersistentJobQueueProvider
    {
        private readonly MongoStorageOptions _storageOptions;

        public MongoJobQueueProvider(MongoStorageOptions storageOptions)
        {
            if (storageOptions == null)
            {
                throw new ArgumentNullException(nameof(storageOptions));
            }

            _storageOptions = storageOptions;
        }

        public IPersistentJobQueue GetJobQueue(HangfireDbContext connection)
        {
            return new MongoJobQueue(connection, _storageOptions);
        }

        public IPersistentJobQueueMonitoringApi GetJobQueueMonitoringApi(HangfireDbContext connection)
        {
            return new MongoJobQueueMonitoringApi(connection);
        }
    }
#pragma warning restore 1591
}