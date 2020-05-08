using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Hangfire.Mongo
{
    /// <summary>
    /// Represents Hangfire storage options for Cosmos DB for MongoDB API implementation
    /// </summary>
    public class CosmosMongoStorageOptions : MongoStorageOptions
    {
        /// <summary>
        /// Constructs storage options with default parameters
        /// </summary>
        public CosmosMongoStorageOptions()
            :base()
        {
            CosmosHourlyTtl = 6;
        }

        /// <summary>
        /// Constructs storage options with a custom TTL (hourly)
        /// </summary>
        /// <param name="cosmosHourlyTTL"></param>
        public CosmosMongoStorageOptions(int cosmosHourlyTTL)
            : base()
        {
            CosmosHourlyTtl = cosmosHourlyTTL;
        }

        /// <summary>
        /// Comos TTL for "signals" and "notifications" collections
        /// </summary>
        public int CosmosHourlyTtl { get; set; }

    }
}
