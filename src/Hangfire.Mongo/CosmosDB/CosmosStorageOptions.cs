using Hangfire.Mongo.Migration.Strategies;
using Hangfire.Mongo.Migration.Strategies.Backup;

namespace Hangfire.Mongo.CosmosDB
{
    /// <summary>
    /// Cosmos storage options
    /// </summary>
    public class CosmosStorageOptions : MongoStorageOptions
    {
        /// <summary>
        /// ctor
        /// </summary>
        public CosmosStorageOptions()
        {
            UseNotificationsCollection = false;
            CheckConnection = false;
            UseTransactions = false;
            MigrationOptions = new MongoMigrationOptions
            {
                BackupStrategy = new NoneMongoBackupStrategy(),
                MigrationStrategy = new DropMongoMigrationStrategy()
            };
        }
    }
}