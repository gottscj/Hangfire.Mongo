using MongoDB.Driver;

namespace Hangfire.Mongo.DocumentDB
{
    /// <summary>
    /// Represents extensions to configure CosmosDB storage for Hangfire
    /// </summary>
    public static class DocumentDbBootstrapperConfigurationExtensions
    {
        /// <summary>
        /// Configure Hangfire to use CosmosDB storage
        /// </summary>
        /// <param name="configuration">Configuration</param>
        /// <param name="mongoClient">Client for Mongo</param>
        /// <param name="databaseName">Name of database</param>
        /// <param name="storageOptions">Storage options</param>
        /// <returns></returns>
        public static DocumentDbStorage UseDocumentDbStorage(this IGlobalConfiguration configuration,
            IMongoClient mongoClient,
            string databaseName,
            DocumentDbStorageOptions storageOptions)
        {
            var storage = new DocumentDbStorage(mongoClient, databaseName, storageOptions);

            configuration.UseStorage(storage);

            return storage;
        }
    }
}