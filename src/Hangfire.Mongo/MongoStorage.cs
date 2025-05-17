﻿using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Threading;
using Hangfire.Annotations;
using Hangfire.Logging;
using Hangfire.Mongo.Database;
using Hangfire.Server;
using Hangfire.Storage;
using MongoDB.Bson;
using MongoDB.Driver;

namespace Hangfire.Mongo
{
    /// <summary>
    /// Hangfire Job Storage implementation for Mongo database
    /// </summary>
    public class MongoStorage : JobStorage
    {
        /// <summary>
        /// Name of hangfire bb
        /// </summary>
        protected readonly string DatabaseName;

        /// <summary>
        /// Mongo client instance used for hangfire mongo
        /// </summary>
        protected readonly IMongoClient MongoClient;

        /// <summary>
        /// Storage options
        /// </summary>
        protected readonly MongoStorageOptions StorageOptions;

        /// <summary>
        /// DB context
        /// </summary>
        protected readonly HangfireDbContext HangfireDbContext;

        /// <summary>
        /// Enabled Hangfire features. To change enabled features, inherit this class and override 'HasFeature' method
        /// </summary>
        public ReadOnlyDictionary<string, bool> Features { get; protected set; } = new(
            new Dictionary<string, bool>(StringComparer.OrdinalIgnoreCase)
            {
                {JobStorageFeatures.ExtendedApi, true},
                {JobStorageFeatures.JobQueueProperty, true},
                {JobStorageFeatures.ProcessesInsteadOfComponents, true},
                {JobStorageFeatures.Connection.BatchedGetFirstByLowest, true},
                {JobStorageFeatures.Connection.GetUtcDateTime, true},
                {JobStorageFeatures.Connection.GetSetContains, true},
                {JobStorageFeatures.Connection.LimitedGetSetCount, true},
                {JobStorageFeatures.Transaction.AcquireDistributedLock, true},
                {JobStorageFeatures.Transaction.CreateJob, true},
                {JobStorageFeatures.Transaction.SetJobParameter, true},
                {JobStorageFeatures.Monitoring.DeletedStateGraphs, true},
                {JobStorageFeatures.Monitoring.AwaitingJobs, true}
            });

        /// <summary>
        /// Constructs Job Storage by Mongo client settings and name
        /// </summary>
        /// <param name="mongoClientSettings">Client settings for MongoDB</param>
        /// <param name="databaseName">Database name</param>
        public MongoStorage(MongoClientSettings mongoClientSettings, string databaseName)
            : this(mongoClientSettings, databaseName, new MongoStorageOptions())
        {
        }

        /// <summary>
        /// Constructs Job Storage by Mongo client settings, name and options
        /// </summary>
        /// <param name="mongoClientSettings">Client settings for MongoDB</param>
        /// <param name="databaseName">Database name</param>
        /// <param name="storageOptions">Storage options</param>
        public MongoStorage(MongoClientSettings mongoClientSettings, string databaseName,
            MongoStorageOptions storageOptions)
            : this(new MongoClient(mongoClientSettings), databaseName, storageOptions)
        {
        }

        /// <summary>
        /// Constructs Job Storage by Mongo client, name and options
        /// </summary>
        /// <param name="mongoClient"></param>
        /// <param name="databaseName"></param>
        /// <param name="storageOptions"></param>
        /// <exception cref="ArgumentNullException"></exception>
        public MongoStorage(IMongoClient mongoClient, string databaseName, MongoStorageOptions storageOptions)
        {
            if (string.IsNullOrWhiteSpace(databaseName))
            {
                throw new ArgumentNullException(nameof(databaseName));
            }

            StorageOptions = storageOptions ?? throw new ArgumentNullException(nameof(storageOptions));

            if (storageOptions.CheckQueuedJobsStrategy == CheckQueuedJobsStrategy.TailNotificationsCollection &&
                storageOptions.SupportsCappedCollection == false)
            {
                throw new NotSupportedException(
                    $"{nameof(MongoStorageOptions.CheckQueuedJobsStrategy)}, cannot be {CheckQueuedJobsStrategy.TailNotificationsCollection}" +
                    $" if {nameof(MongoStorageOptions.SupportsCappedCollection)} is false"
                );
            }

            DatabaseName = databaseName;
            MongoClient = mongoClient ?? throw new ArgumentNullException(nameof(mongoClient));

            HangfireDbContext =
                StorageOptions.Factory.CreateDbContext(mongoClient, databaseName, storageOptions.Prefix);

            if (StorageOptions.CheckConnection)
            {
                CheckConnection();
            }

            if (StorageOptions.ByPassMigration)
            {
                return;
            }

            var migrationManager = storageOptions
                .Factory
                .CreateMongoMigrationManager(storageOptions, HangfireDbContext.Database);

            if (!migrationManager.NeedsMigration())
            {
                return;
            }

            using var migrationLock = storageOptions
                .Factory
                .CreateMigrationLock(HangfireDbContext.Database, storageOptions);
            migrationLock.AcquireLock();
            migrationManager.MigrateUp();
        }

        private void CheckConnection()
        {
            using (var cts = new CancellationTokenSource(StorageOptions.ConnectionCheckTimeout))
            {
                try
                {
                    HangfireDbContext.Database.RunCommand((Command<BsonDocument>) "{ping:1}",
                        cancellationToken: cts.Token);
                }
                catch (Exception e)
                {
                    throw new MongoConnectException(HangfireDbContext, CreateObscuredConnectionString(),
                        StorageOptions.ConnectionCheckTimeout, e);
                }
            }
        }

        /// <inheritdoc/>
        public override bool HasFeature([NotNull] string featureId)
        {
            if (featureId == null) throw new ArgumentNullException(nameof(featureId));

            return Features.TryGetValue(featureId, out var isSupported)
                ? isSupported
                : base.HasFeature(featureId);
        }


        /// <summary>
        /// Returns Monitoring API object
        /// </summary>
        /// <returns>Monitoring API object</returns>
        public override IMonitoringApi GetMonitoringApi()
        {
            return StorageOptions.Factory.CreateMongoMonitoringApi(HangfireDbContext);
        }

        /// <summary>
        /// Returns storage connection
        /// </summary>
        /// <returns>Storage connection</returns>
        public override IStorageConnection GetConnection()
        {
            return StorageOptions.Factory.CreateMongoConnection(HangfireDbContext, StorageOptions);
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
                case CheckQueuedJobsStrategy.TailNotificationsCollection:
                    if (StorageOptions.SupportsCappedCollection)
                    {
                        yield return StorageOptions.Factory.CreateMongoNotificationObserver(HangfireDbContext,
                            StorageOptions);
                    }

                    break;
                case CheckQueuedJobsStrategy.Poll:
                    break;
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
                case CheckQueuedJobsStrategy.TailNotificationsCollection:
                    if (StorageOptions.SupportsCappedCollection)
                    {
                        yield return StorageOptions.Factory.CreateMongoNotificationObserver(HangfireDbContext,
                            StorageOptions);
                    }

                    break;
            }
        }

        /// <summary>
        /// Writes storage options to log
        /// </summary>
        /// <param name="logger">Logger</param>
        public override void WriteOptionsToLog(ILog logger)
        {
            logger.Info("Using the following options for Mongo DB job storage:");
            logger.InfoFormat("    Prefix: {0}.", StorageOptions.Prefix);
        }

        /// <summary>
        /// Returns text representation of the object
        /// </summary>
        public override string ToString()
        {
            return
                $"Connection string: {CreateObscuredConnectionString()}, database name: {DatabaseName}, prefix: {StorageOptions.Prefix}";
        }

        private string CreateObscuredConnectionString()
        {
            // Obscure the username and password for display purposes
            string obscuredConnectionString = "mongodb://";
            if (MongoClient.Settings != null && MongoClient.Settings.Servers != null)
            {
                var servers = string.Join(",", MongoClient.Settings.Servers.Select(s => $"{s.Host}:{s.Port}"));
                obscuredConnectionString = $"mongodb://<username>:<password>@{servers}";
            }

            return obscuredConnectionString;
        }
    }
}