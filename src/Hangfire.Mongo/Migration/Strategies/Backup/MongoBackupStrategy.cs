using MongoDB.Driver;

namespace Hangfire.Mongo.Migration.Strategies.Backup
{
    /// <summary>
    /// Strategy for backing up hangfire data before migration
    /// </summary>
    public abstract class MongoBackupStrategy
    {
        /// <summary>
        /// Executes backup routine
        /// </summary>
        public virtual void Backup(MongoStorageOptions storageOptions, IMongoDatabase database, MongoSchema fromSchema, MongoSchema toSchema)
        {
            
        }
    }
}