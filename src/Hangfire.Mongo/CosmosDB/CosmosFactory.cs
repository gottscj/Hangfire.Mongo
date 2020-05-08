using Hangfire.Mongo.Database;

namespace Hangfire.Mongo.CosmosDB
{
    /// <summary>
    /// CosmosDB factory
    /// </summary>
    public class CosmosFactory : MongoFactory
    {
        /// <summary>
        /// ctor
        /// </summary>
        /// <param name="storageOptions"></param>
        public CosmosFactory(CosmosStorageOptions storageOptions) : base(storageOptions)
        {
        }

        /// <inheritdoc />
        public override MongoJobFetcher CreateMongoJobFetcher(HangfireDbContext dbContext)
        {
            return new CosmosJobFetcher(dbContext, StorageOptions, JobQueueSemaphore);
        }
    }
}