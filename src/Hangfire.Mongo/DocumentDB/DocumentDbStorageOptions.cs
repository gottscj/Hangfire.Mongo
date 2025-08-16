using Hangfire.Mongo.UtcDateTime;

namespace Hangfire.Mongo.DocumentDB
{
    /// <summary>
    /// Storage options preconfigured for use with AWS DocumentDB servers
    /// </summary>
    public class DocumentDbStorageOptions : MongoStorageOptions
    {
        /// <summary>
        /// Constructs DocumentDB-specific storage options.
        /// </summary>
        public DocumentDbStorageOptions()
            : base()
        {
            // Restrict UTC date/time strategy to the isMaster command, which is supported by DocumentDB unprivileged users.
            UtcDateTimeStrategies =
            [
                new IsMasterUtcDateTimeStrategy()
            ];
        }
    }
}
