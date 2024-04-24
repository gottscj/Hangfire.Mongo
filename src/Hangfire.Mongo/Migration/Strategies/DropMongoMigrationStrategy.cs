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
        /// Drop strategy will not validate schemas as it drops all collections anyway and migrates from scratch
        /// </summary>
        /// <param name="requiredSchema"></param>
        /// <param name="currentSchema"></param>
        public override void ValidateSchema(MongoSchema requiredSchema, MongoSchema currentSchema)
        {
            // nop
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
            var existingCollectionNames =
                MongoMigrationUtils.ExistingHangfireCollectionNames(database, fromSchema, StorageOptions);
            foreach (var collectionName in existingCollectionNames)
            {
                database.DropCollection(collectionName);
            }
        }
    }
}
