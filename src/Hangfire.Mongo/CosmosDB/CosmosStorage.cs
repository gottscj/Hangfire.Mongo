using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
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
            Features = new ReadOnlyDictionary<string, bool>(
                new Dictionary<string, bool>(StringComparer.OrdinalIgnoreCase)
                {
                    {JobStorageFeatures.ExtendedApi, true},
                    {JobStorageFeatures.JobQueueProperty, true},
                    {JobStorageFeatures.Connection.BatchedGetFirstByLowest, true},
                    {JobStorageFeatures.Connection.GetUtcDateTime, false},
                    {JobStorageFeatures.Connection.GetSetContains, true},
                    {JobStorageFeatures.Connection.LimitedGetSetCount, true},
                    {JobStorageFeatures.Transaction.AcquireDistributedLock, true},
                    {JobStorageFeatures.Transaction.CreateJob, true},
                    {JobStorageFeatures.Transaction.SetJobParameter, true},
                    {JobStorageFeatures.Monitoring.DeletedStateGraphs, true},
                    {JobStorageFeatures.Monitoring.AwaitingJobs, true}
                });
        }
    }
}