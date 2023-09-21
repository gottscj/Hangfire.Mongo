using Hangfire.Mongo.Database;

namespace Hangfire.Mongo.CosmosDB;

/// <inheritdoc />
public class CosmosFactory : MongoFactory
{
    /// <inheritdoc />
    public override MongoWriteOnlyTransaction CreateMongoWriteOnlyTransaction(HangfireDbContext dbContext,
        MongoStorageOptions storageOptions)
    {
        return new CosmosDbWriteOnlyTransaction(dbContext, storageOptions);
    }

    /// <inheritdoc />
    public override MongoConnection CreateMongoConnection(HangfireDbContext dbContext, MongoStorageOptions storageOptions)
    {
        return new CosmosConnection(dbContext, storageOptions);
    }

    /// <inheritdoc />
    public override MongoJobQueueWatcher CreateMongoJobQueueWatcher(HangfireDbContext dbContext, MongoStorageOptions storageOptions)
    {
        return new CosmosQueueWatcher(dbContext, storageOptions, JobQueueSemaphore);
    }
}