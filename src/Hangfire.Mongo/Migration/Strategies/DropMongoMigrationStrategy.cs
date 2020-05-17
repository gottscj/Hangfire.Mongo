using MongoDB.Driver;

#pragma warning disable 1591
namespace Hangfire.Mongo.Migration.Strategies
{
#pragma warning restore 1591

    /// <summary>
    /// Drop DB before migration
    /// </summary>
    public class DropMongoMigrationStrategy : MongoMigrationStrategy
    {
        /// <summary>
        /// ctor
        /// </summary>
        public DropMongoMigrationStrategy() 
            : this(new MongoMigrationContext())
        {
        }
        
        /// <summary>
        /// ctor
        /// </summary>
        /// <param name="mongoMigrationContext"></param>
        public DropMongoMigrationStrategy(IMongoMigrationContext mongoMigrationContext) 
            : base(mongoMigrationContext)
        {
        }

        /// <summary>
        /// Execute migration from "None"
        /// </summary>
        /// <param name="database"></param>
        /// <param name="fromSchema"></param>
        /// <param name="toSchema"></param>
        protected override void ExecuteMigration(IMongoDatabase database, MongoSchema fromSchema, MongoSchema toSchema)
        {
            base.ExecuteMigration(database, MongoSchema.None, toSchema);
        }

        /// <summary>
        /// drop all hangfire collections
        /// </summary>
        /// <param name="database"></param>
        /// <param name="fromSchema"></param>
        /// <param name="toSchema"></param>
        protected override void ExecuteStrategy(IMongoDatabase database, MongoSchema fromSchema, MongoSchema toSchema)
        {
            foreach (var collectionName in MongoMigrationUtils.ExistingHangfireCollectionNames(database, fromSchema, StorageOptions))
            {
                database.DropCollection(collectionName);
            }
        }
    }
}
