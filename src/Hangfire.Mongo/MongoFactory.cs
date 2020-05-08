using System;
using Hangfire.Mongo.Database;
using Hangfire.Mongo.DistributedLock;
using MongoDB.Bson;
using MongoDB.Driver;

namespace Hangfire.Mongo
{
    /// <summary>
    /// Factory for creating dynamic instances
    /// </summary>
    public class MongoFactory
    {
#pragma warning disable 1591
        protected readonly MongoStorageOptions StorageOptions;
#pragma warning restore 1591

        /// <summary>
        /// ctor
        /// </summary>
        /// <param name="storageOptions"></param>
        public MongoFactory(MongoStorageOptions storageOptions)
        {
            StorageOptions = storageOptions;
        }
        
        /// <summary>
        /// JobQueueSemaphore instance used in Hangfire.Mongo
        /// </summary>
        public IJobQueueSemaphore JobQueueSemaphore { get; set; } = new JobQueueSemaphore();

        /// <summary>
        /// DistributedLockMutex instance used in Hangfire.Mongo
        /// </summary>
        public IDistributedLockMutex DistributedLockMutex { get; set; } = new DistributedLockMutex();
        
        /// <summary>
        /// Factory method to create HangfireDbContext instance
        /// </summary>
        /// <param name="mongoClient"></param>
        /// <param name="databaseName"></param>
        /// <returns></returns>
        public virtual HangfireDbContext CreateDbContext(MongoClient mongoClient, string databaseName)
        {
            return new HangfireDbContext(mongoClient, databaseName, StorageOptions.Prefix);
        }
        
        /// <summary>
        /// Creates MongoJobFetcher instance
        /// </summary>
        /// <param name="dbContext"></param>
        /// <returns></returns>
        public virtual MongoJobFetcher CreateMongoJobFetcher(HangfireDbContext dbContext)
        {
            return new MongoJobFetcher(dbContext, StorageOptions, JobQueueSemaphore);
        }
        
        /// <summary>
        /// Creates MongoConnection instance
        /// </summary>
        /// <param name="dbContext"></param>
        /// <returns></returns>
        public virtual MongoConnection CreateMongoConnection(HangfireDbContext dbContext)
        {
            return new MongoConnection(dbContext, StorageOptions);
        }

        /// <summary>
        /// Creates new MongoWriteOnlyTransaction instance
        /// </summary>
        /// <param name="dbContext"></param>
        /// <returns></returns>
        public virtual MongoWriteOnlyTransaction CreateMongoWriteOnlyTransaction(HangfireDbContext dbContext)
        {
            return new MongoWriteOnlyTransaction(dbContext);
        }

        /// <summary>
        /// Creates new MongoDistributedLock
        /// </summary>
        /// <param name="resource"></param>
        /// <param name="timeout"></param>
        /// <param name="dbContext"></param>
        /// <returns></returns>
        public virtual MongoDistributedLock CreateMongoDistributedLock(string resource, TimeSpan timeout, HangfireDbContext dbContext)
        {
            return new MongoDistributedLock($"Hangfire:{resource}", timeout, dbContext, StorageOptions, DistributedLockMutex); 
        }
        
        /// <summary>
        /// Create MongoFetchedJob instance
        /// </summary>
        /// <param name="dbContext"></param>
        /// <param name="id"></param>
        /// <param name="jobId"></param>
        /// <param name="queue"></param>
        /// <returns></returns>
        public virtual MongoFetchedJob CreateFetchedJob(HangfireDbContext dbContext, ObjectId id, ObjectId jobId,
            string queue)
        {
            return new MongoFetchedJob(dbContext, id, jobId, queue);
        }

        /// <summary>
        /// Create MongoMonitoringApi instance
        /// </summary>
        /// <param name="dbContext"></param>
        public virtual MongoMonitoringApi CreateMongoMonitoringApi(HangfireDbContext dbContext)
        {
            return new MongoMonitoringApi(dbContext);
        }

        /// <summary>
        /// Creates MongoNotificationObserver instance
        /// </summary>
        /// <param name="dbContext"></param>
        /// <returns></returns>
        public virtual MongoNotificationObserver CreateMongoNotificationObserver(HangfireDbContext dbContext)
        {
            return new MongoNotificationObserver(dbContext, JobQueueSemaphore, DistributedLockMutex);
        }

        /// <summary>
        /// Creates MongoExpirationManager instance
        /// </summary>
        /// <param name="dbContext"></param>
        /// <returns></returns>
        public virtual MongoExpirationManager CreateMongoExpirationManager(HangfireDbContext dbContext)
        {
            return new MongoExpirationManager(dbContext, StorageOptions);
        }
    }
}