namespace Hangfire.Mongo.CosmosDB
{
    /// <summary>
    /// Storage options for use with CosmosDB
    /// </summary>
    public class CosmosStorageOptions : MongoStorageOptions
    {
        /// <summary>
        /// ctor
        /// </summary>
        public CosmosStorageOptions()
        {
            Factory = new CosmosFactory(this);
        }
    }
}