using MongoDB.Driver;

namespace Hangfire.Mongo.CosmosDB
{
    /// <summary>
    /// Cosmos DB storage
    /// </summary>
    public class CosmosStorage : MongoStorage
    {
        /// <summary>
        /// Storage for CosmosDB
        /// </summary>
        /// <param name="mongoClient"></param>
        /// <param name="databaseName"></param>
        /// <param name="storageOptions"></param>
        public CosmosStorage(MongoClient mongoClient, string databaseName, CosmosStorageOptions storageOptions) 
            : base(mongoClient, databaseName, storageOptions)
        {
        }
    }
}