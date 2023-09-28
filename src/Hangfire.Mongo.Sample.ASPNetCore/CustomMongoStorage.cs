using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using Hangfire.Storage;
using MongoDB.Driver;

namespace Hangfire.Mongo.Sample.ASPNetCore;

public class CustomMongoStorage : MongoStorage
{
    public CustomMongoStorage(IMongoClient mongoClient, string databaseName, MongoStorageOptions storageOptions) : base(
        mongoClient, databaseName, storageOptions)
    {
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