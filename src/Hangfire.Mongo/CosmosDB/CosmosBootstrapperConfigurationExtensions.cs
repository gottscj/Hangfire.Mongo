using MongoDB.Driver;

namespace Hangfire.Mongo.CosmosDB
{
    /// <summary>
    /// Represents extensions to configure CosmosDB storage for Hangfire
    /// </summary>
    public static class CosmosBootstrapperConfigurationExtensions
    {
        /// <summary>
        /// Configure Hangfire to use CosmosDB storage
        /// </summary>
        /// <param name="configuration">Configuration</param>
        /// <param name="mongoClient">Client for Mongo</param>
        /// <param name="databaseName">Name of database at Cosmos server</param>
        /// <param name="storageOptions">Storage options</param>
        /// <returns></returns>
        public static CosmosStorage UseCosmosStorage(this IGlobalConfiguration configuration,
            MongoClient mongoClient,
            string databaseName,
            CosmosStorageOptions storageOptions)
        {
            var storage = new CosmosStorage(mongoClient, databaseName, storageOptions);

            configuration.UseStorage(storage);

            return storage;
        }
    }
}