﻿using System;
using Hangfire.Mongo.Database;
using Hangfire.Mongo.DistributedLock;
using Hangfire.Mongo.Migration;
using MongoDB.Bson;
using MongoDB.Driver;

namespace Hangfire.Mongo
{
    /// <summary>
    /// Factory for creating dynamic instances
    /// </summary>
    public class MongoFactory
    {
        /// <summary>
        /// JobQueueSemaphore instance used in Hangfire.Mongo
        /// </summary>
        public IJobQueueSemaphore JobQueueSemaphore { get; set; } = new JobQueueSemaphore();

        /// <summary>
        /// Creates new Migration manager
        /// </summary>
        /// <param name="storageOptions"></param>
        /// <param name="database"></param>
        /// <returns></returns>
        public virtual MongoMigrationManager CreateMongoMigrationManager(MongoStorageOptions storageOptions,
            IMongoDatabase database)
        {
            return new MongoMigrationManager(storageOptions, database);
        }

        /// <summary>
        /// Creates migration lock
        /// </summary>
        /// <param name="database"></param>
        /// <param name="storageOptions"></param>
        /// <returns></returns>
        public virtual MigrationLock CreateMigrationLock(IMongoDatabase database, MongoStorageOptions storageOptions)
        {
            return new MigrationLock(database, storageOptions);
        }
        
        /// <summary>
        /// Factory method to create HangfireDbContext instance
        /// </summary>
        /// <param name="mongoClient"></param>
        /// <param name="databaseName"></param>
        /// <param name="prefix"></param>
        /// <returns></returns>
        public virtual HangfireDbContext CreateDbContext(IMongoClient mongoClient, string databaseName, string prefix)
        {
            return new HangfireDbContext(mongoClient, databaseName, prefix);
        }

        /// <summary>
        /// Creates MongoJobFetcher instance
        /// </summary>
        /// <param name="dbContext"></param>
        /// <param name="storageOptions"></param>
        /// <returns></returns>
        public virtual MongoJobFetcher CreateMongoJobFetcher(HangfireDbContext dbContext, MongoStorageOptions storageOptions)
        {
            return new MongoJobFetcher(dbContext, storageOptions, JobQueueSemaphore);
        }

        /// <summary>
        /// Creates MongoConnection instance
        /// </summary>
        /// <param name="dbContext"></param>
        /// <param name="storageOptions"></param>
        /// <returns></returns>
        public virtual MongoConnection CreateMongoConnection(HangfireDbContext dbContext, MongoStorageOptions storageOptions)
        {
            return new MongoConnection(dbContext, storageOptions);
        }

        /// <summary>
        /// Creates new MongoWriteOnlyTransaction instance
        /// </summary>
        /// <param name="dbContext"></param>
        /// <param name="storageOptions"></param>
        /// <returns></returns>
        public virtual MongoWriteOnlyTransaction CreateMongoWriteOnlyTransaction(HangfireDbContext dbContext, MongoStorageOptions storageOptions)
        {
            return new MongoWriteOnlyTransaction(dbContext, storageOptions);
        }

        /// <summary>
        /// Creates new MongoDistributedLock
        /// </summary>
        /// <param name="resource"></param>
        /// <param name="timeout"></param>
        /// <param name="dbContext"></param>
        /// <param name="storageOptions"></param>
        /// <returns></returns>
        public virtual MongoDistributedLock CreateMongoDistributedLock(string resource, TimeSpan timeout, HangfireDbContext dbContext, MongoStorageOptions storageOptions)
        {
            return new MongoDistributedLock($"Hangfire:{resource}", timeout, dbContext, storageOptions); 
        }

        /// <summary>
        /// Create MongoFetchedJob instance
        /// </summary>
        /// <param name="dbContext"></param>
        /// <param name="storageOptions"></param>
        /// <param name="fetchedAt"></param>
        /// <param name="id"></param>
        /// <param name="jobId"></param>
        /// <param name="queue"></param>
        /// <returns></returns>
        public virtual MongoFetchedJob CreateFetchedJob(
            HangfireDbContext dbContext, 
            MongoStorageOptions storageOptions,
            DateTime fetchedAt,
            ObjectId id, 
            ObjectId jobId,
            string queue)
        {
            return new MongoFetchedJob(dbContext, storageOptions, fetchedAt, id, jobId, queue);
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
        /// <param name="storageOptions"></param>
        /// <returns></returns>
        public virtual MongoNotificationObserver CreateMongoNotificationObserver(HangfireDbContext dbContext, MongoStorageOptions storageOptions)
        {
            return new MongoNotificationObserver(dbContext, storageOptions, JobQueueSemaphore);
        }

        /// <summary>
        /// Creates MongoJobQueue watcher instance
        /// </summary>
        /// <param name="dbContext"></param>
        /// <param name="storageOptions"></param>
        /// <returns></returns>
        public virtual MongoJobQueueWatcher CreateMongoJobQueueWatcher(HangfireDbContext dbContext, MongoStorageOptions storageOptions)
        {
            return new MongoJobQueueWatcher(dbContext, storageOptions, JobQueueSemaphore);
        }

        /// <summary>
        /// Creates MongoExpirationManager instance
        /// </summary>
        /// <param name="dbContext"></param>
        /// <param name="storageOptions"></param>
        /// <returns></returns>
        public virtual MongoExpirationManager CreateMongoExpirationManager(HangfireDbContext dbContext, MongoStorageOptions storageOptions)
        {
            return new MongoExpirationManager(dbContext, storageOptions);
        }
    }
}