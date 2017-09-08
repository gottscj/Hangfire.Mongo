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
            _storageOptions = storageOptions ?? throw new ArgumentNullException(nameof(storageOptions));
        }

        public IPersistentJobQueue GetJobQueue(HangfireDbContext database)
        {
            return new MongoJobQueue(database, _storageOptions);
        }

        public IPersistentJobQueueMonitoringApi GetJobQueueMonitoringApi(HangfireDbContext database)
        {
            return new MongoJobQueueMonitoringApi(database);
        }
    }
#pragma warning restore 1591
}