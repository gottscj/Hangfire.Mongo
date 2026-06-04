using Hangfire.Mongo.Database;

namespace Hangfire.Mongo;

/// <inheritdoc />
public class AsyncMongoFactory : MongoFactory
{
    /// <inheritdoc />
    public override MongoJobFetcher CreateMongoJobFetcher(HangfireDbContext dbContext,
        MongoStorageOptions storageOptions)
    {
        return new AsyncMongoJobFetcher(dbContext, storageOptions, JobQueueSemaphore);
    }

    /// <inheritdoc />
    public override MongoConnection CreateMongoConnection(HangfireDbContext dbContext,
        MongoStorageOptions storageOptions)
    {
        return new AsyncMongoConnection(dbContext, storageOptions);
    }

    /// <inheritdoc />
    public override MongoJobQueueWatcher CreateMongoJobQueueWatcher(HangfireDbContext dbContext,
        MongoStorageOptions storageOptions)
    {
        return new AsyncMongoJobQueueWatcher(dbContext, storageOptions, JobQueueSemaphore);
    }

    /// <inheritdoc />
    public override MongoNotificationObserver CreateMongoNotificationObserver(HangfireDbContext dbContext,
        MongoStorageOptions storageOptions)
    {
        return new AsyncMongoNotificationObserver(dbContext, storageOptions, JobQueueSemaphore);
    }

    /// <inheritdoc />
    public override MongoExpirationManager CreateMongoExpirationManager(HangfireDbContext dbContext,
        MongoStorageOptions storageOptions)
    {
        return new AsyncMongoExpirationManager(dbContext, storageOptions);
    }
}