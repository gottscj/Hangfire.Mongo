using System.Collections.Generic;
using System.Linq;
using MongoDB.Driver;

namespace Hangfire.Mongo.Migration
{
    /// <summary>
    /// Utility methods used for migration
    /// </summary>
    public class MongoMigrationUtils
    {
        /// <summary>
        /// Find hangfire collection namespaces by reflecting over properties on database.
        /// </summary>
        public static IEnumerable<string> ExistingHangfireCollectionNames(IMongoDatabase database, MongoSchema schema, MongoStorageOptions storageOptions)
        {
            var existingCollectionNames = ExistingDatabaseCollectionNames(database).ToList();
            return schema.CollectionNames(storageOptions.Prefix).Where(c => existingCollectionNames.Contains(c));
        }
        
        /// <summary>
        /// Gets the existing collection names from database
        /// </summary>
        public static IEnumerable<string> ExistingDatabaseCollectionNames(IMongoDatabase database)
        {
            return database.ListCollections().ToList().Select(c => c["name"].AsString);
        }
        
        /// <summary>
        /// Generate the name of tha tbackup collection based on the original collection name and schema.
        /// </summary>
        public static string GetBackupCollectionName(string collectionName, MongoSchema schema, MongoStorageOptions storageOptions)
        {
            return $@"{collectionName}.{(int)schema}.{storageOptions.MigrationOptions.BackupPostfix}";
        }
        
        /// <summary>
        /// Generate the name of that backup collection based on the original collection name and schema.
        /// </summary>
        public static string GetBackupDatabaseName(string databaseName, MongoSchema schema, MongoStorageOptions storageOptions)
        {
            return $@"{databaseName}-{(int)schema}-{storageOptions.MigrationOptions.BackupPostfix}";
        }
    }
}