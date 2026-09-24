using System;
using Hangfire.Mongo.Database;
using Hangfire.Mongo.DistributedLock;
using MongoDB.Bson;

namespace Hangfire.Mongo
{
    /// <summary>
    /// Factory which keeps fetched jobs and distributed locks alive with asynchronous heartbeats,
    /// so heartbeats do not block thread-pool threads while waiting for the database.
    /// Opt in by setting <see cref="MongoStorageOptions.Factory"/> to an instance of this class.
    /// </summary>
    public class AsyncMongoFactory : MongoFactory
    {
        /// <inheritdoc />
        public override MongoDistributedLock CreateMongoDistributedLock(string resource, TimeSpan timeout,
            HangfireDbContext dbContext, MongoStorageOptions storageOptions)
        {
            return new AsyncMongoDistributedLock($"Hangfire:{resource}", timeout, dbContext, storageOptions);
        }

        /// <inheritdoc />
        public override MongoFetchedJob CreateFetchedJob(
            HangfireDbContext dbContext,
            MongoStorageOptions storageOptions,
            DateTime fetchedAt,
            string fetchToken,
            ObjectId id,
            ObjectId jobId,
            string queue)
        {
            return new AsyncMongoFetchedJob(dbContext, storageOptions, fetchedAt, fetchToken, id, jobId, queue);
        }
    }
}
