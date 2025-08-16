using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using Hangfire.Server;
using Hangfire.Storage;
using MongoDB.Driver;

namespace Hangfire.Mongo.CosmosDB
{
    /// <summary>
    /// Cosmos DB storage
    /// </summary>
    public class CosmosStorage : MongoStorage
    {
        /// <summary>
        /// Storage for CosmosDB
        /// </summary>
        /// <param name="mongoClient"></param>
        /// <param name="databaseName"></param>
        /// <param name="storageOptions"></param>
        public CosmosStorage(IMongoClient mongoClient, string databaseName, CosmosStorageOptions storageOptions)
            : base(mongoClient, databaseName, storageOptions)
        {
            if (storageOptions.CheckQueuedJobsStrategy == CheckQueuedJobsStrategy.TailNotificationsCollection)
            {
                throw new ArgumentException("CosmosDB does not support capped collections");
            }
        }

        /// <inheritdoc />
        public override IEnumerable<IBackgroundProcess> GetServerRequiredProcesses()
        {
            yield return StorageOptions.Factory.CreateMongoExpirationManager(HangfireDbContext, StorageOptions);
            switch (StorageOptions.CheckQueuedJobsStrategy)
            {
                case CheckQueuedJobsStrategy.Watch:
                    
                    yield return StorageOptions.Factory.CreateMongoJobQueueWatcher(HangfireDbContext, StorageOptions);
                    break;
                case CheckQueuedJobsStrategy.Poll:
                    break;
                case CheckQueuedJobsStrategy.TailNotificationsCollection:
                    throw new NotSupportedException("CosmosDB does not support capped collections");
                default:
                    throw new ArgumentOutOfRangeException();
            }
        }
        
        /// <inheritdoc />
        public override IEnumerable<IBackgroundProcess> GetStorageWideProcesses()
        {
            return [];
        }

        /// <summary>
        /// Returns collection of server components
        /// </summary>
        /// <returns>Collection of server components</returns>
        [Obsolete("Please use the `GetStorageWideProcesses` and/or `GetServerRequiredProcesses` methods instead, and enable `JobStorageFeatures.ProcessesInsteadOfComponents`. Will be removed in 2.0.0.")]
        public override IEnumerable<IServerComponent> GetComponents()
        {
            yield return StorageOptions.Factory.CreateMongoExpirationManager(HangfireDbContext, StorageOptions);
            switch (StorageOptions.CheckQueuedJobsStrategy)
            {
                case CheckQueuedJobsStrategy.Watch:
                    
                    yield return StorageOptions.Factory.CreateMongoJobQueueWatcher(HangfireDbContext, StorageOptions);
                    break;
                case CheckQueuedJobsStrategy.Poll:
                    break;
                case CheckQueuedJobsStrategy.TailNotificationsCollection:
                    throw new NotSupportedException("CosmosDB does not support capped collections");
                default:
                    throw new ArgumentOutOfRangeException();
            }
        }
    }
}